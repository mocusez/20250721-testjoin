#include "columnar_hashjoin.h"
#include "memory.h"
#include "xxhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#define TABLE_SIZE 1024

// Hash table entry - using open addressing
typedef struct Entry {
    int row_index;              // Row index
    int key;                    // Key value
    int is_occupied;            // Whether the slot is occupied
    int is_deleted;             // Whether the slot is deleted (for deletion operations)
} Entry;

// Hash table structure
typedef struct {
    Entry buckets[TABLE_SIZE];  // Store entry array directly, without using pointers
    int count;                  // Current number of stored elements
} HashTable;

unsigned int hash_key(int key) {
    return (unsigned int)(XXH3_64bits(&key, sizeof(int)) % TABLE_SIZE);
}

// Linear probing to find the next position
int linear_probe(HashTable *table, int key, int start_idx) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        int idx = (start_idx + i) % TABLE_SIZE;
        if (!table->buckets[idx].is_occupied || 
            (table->buckets[idx].is_occupied && table->buckets[idx].key == key)) {
            return idx;
        }
    }
    return -1; // Table is full
}

// Insert row index into hash table
void insert_hash(HashTable *table, int key, int row_index) {
    if (table->count >= TABLE_SIZE * 0.7) {
        printf("Warning: Hash table is getting full\n");
        return;
    }
    
    unsigned int start_idx = hash_key(key);
    int idx = linear_probe(table, key, start_idx);
    
    if (idx != -1) {
        table->buckets[idx].key = key;
        table->buckets[idx].row_index = row_index;
        table->buckets[idx].is_occupied = 1;
        table->buckets[idx].is_deleted = 0;
        table->count++;
    }
}

// Search hash table - return matching entry index, -1 means not found
int lookup_hash(HashTable *table, int key) {
    unsigned int start_idx = hash_key(key);
    
    for (int i = 0; i < TABLE_SIZE; i++) {
        int idx = (start_idx + i) % TABLE_SIZE;
        
        if (!table->buckets[idx].is_occupied && !table->buckets[idx].is_deleted) {
            return -1;
        }
        
        if (table->buckets[idx].is_occupied && 
            !table->buckets[idx].is_deleted && 
            table->buckets[idx].key == key) {
            return idx;
        }
    }
    
    return -1;
}

// Initialize hash table
void init_hash_table(HashTable *table) {
    table->count = 0;
    for (int i = 0; i < TABLE_SIZE; i++) {
        table->buckets[i].is_occupied = 0;
        table->buckets[i].is_deleted = 0;
        table->buckets[i].key = 0;
        table->buckets[i].row_index = 0;
    }
}

// Free hash table memory (clear all entries)
void free_hash_table(HashTable *table) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        table->buckets[i].is_occupied = 0;
        table->buckets[i].is_deleted = 0;
        table->buckets[i].key = 0;
        table->buckets[i].row_index = 0;
    }
    table->count = 0;
}

int is_ndb_value_null(const NDBTableC* table, int column_idx, int row_idx) {
    if (column_idx < 0 || column_idx >= table->num_columns || 
        row_idx < 0 || row_idx >= table->num_rows) {
        return 1; // Out of bounds treated as NULL
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    if (!array->validity) {
        return 0; // No validity bitmap, meaning all are not NULL
    }
    
    int byte_idx = row_idx / 8;
    int bit_idx = row_idx % 8;
    return !(array->validity[byte_idx] & (1 << bit_idx));
}

void set_ndb_value_null(NDBTableC* table, int column_idx, int row_idx) {
    if (column_idx < 0 || column_idx >= table->num_columns || 
        row_idx < 0 || row_idx >= table->num_rows) {
        return;
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    
    // Ensure there is a validity bitmap
    if (!array->validity) {
        int bitmap_size = (table->num_rows + 7) / 8;
        array->validity = (uint8_t*)malloc(bitmap_size);
        memset(array->validity, 0xFF, bitmap_size); // Set all to valid
        array->null_count = 0;
    }
    
    int byte_idx = row_idx / 8;
    int bit_idx = row_idx % 8;
    
    // Check if already NULL
    if (!(array->validity[byte_idx] & (1 << bit_idx))) {
        return; // Already NULL, no need to set again
    }
    
    // Set to NULL
    array->validity[byte_idx] &= ~(1 << bit_idx);
    array->null_count++;
    
    // For string types, need special handling of offsets
    if (array->type_id == 1) {
        // Ensure NULL string offset points to the same position (length 0)
        if (row_idx < array->length - 1) {
            array->offsets[row_idx + 1] = array->offsets[row_idx];
        }
    }
}

// Get integer key value from NDB column
int get_int_key_from_ndb_column(const NDBTableC* table, int column_idx, int row_idx) {
    if (is_ndb_value_null(table, column_idx, row_idx)) {
        return 0; // NULL value returns 0
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    if (array->type_id == 0) { // int32
        int32_t* data = (int32_t*)array->values;
        return data[row_idx];
    }
    
    return 0; // Non-integer types return 0
}

// Get NDB column data
void* get_ndb_column_data(const NDBTableC* table, int column_idx, int row_idx) {
    if (column_idx < 0 || column_idx >= table->num_columns || 
        row_idx < 0 || row_idx >= table->num_rows) {
        return NULL;
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    if (array->type_id == 0) { // int32
        int32_t* data = (int32_t*)array->values;
        return &data[row_idx];
    } else if (array->type_id == 1) { // string
        // For strings, need to return pointer to string data
        static char* str_ptr;
        static int str_len;
        get_ndb_string_value(table, column_idx, row_idx, &str_ptr, &str_len);
        return str_ptr;
    }
    
    return NULL;
}

// Add NDB column data
void add_ndb_column_data(NDBTableC* table, int column_idx, void* data, int row_idx) {
    if (column_idx < 0 || column_idx >= table->num_columns || 
        row_idx < 0 || !data) {
        return;
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    if (array->type_id == 0) { // int32
        int32_t* column_data = (int32_t*)array->values;
        column_data[row_idx] = *(int32_t*)data;
    } else if (array->type_id == 1) { // string
        // For strings, need special handling
        char* str = (char*)data;
        int len = strlen(str);
        set_ndb_string_value(table, column_idx, row_idx, str, len);
    }
    
    // Update table row count
    if (row_idx >= table->num_rows) {
        table->num_rows = row_idx + 1;
    }
}

// Get NDB string value
void get_ndb_string_value(const NDBTableC* table, int column_idx, int row_idx, 
                           char** str_ptr, int* str_len) {
    if (is_ndb_value_null(table, column_idx, row_idx)) {
        *str_ptr = NULL;
        *str_len = 0;
        return;
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    if (array->type_id != 1) { // Not string type
        *str_ptr = NULL;
        *str_len = 0;
        return;
    }
    
    int32_t start = array->offsets[row_idx];
    int32_t end = array->offsets[row_idx + 1];
    *str_len = end - start;
    *str_ptr = (char*)array->values + start;
}

// Set NDB string value
void set_ndb_string_value(NDBTableC* table, int column_idx, int row_idx,
                           const char* str, int str_len) {
    if (column_idx < 0 || column_idx >= table->num_columns || 
        row_idx < 0 || !str) {
        return;
    }
    
    NDBArrayC* array = &table->columns[column_idx];
    if (array->type_id != 1) { // Not string type
        return;
    }
    
    // Simplified implementation: assume values buffer is large enough
    // In actual implementation, dynamic expansion is needed
    char* values_buf = (char*)array->values;
    int current_offset = array->offsets[row_idx];
    
    // Copy string data
    memcpy(values_buf + current_offset, str, str_len);
    
    // Update offsets
    array->offsets[row_idx + 1] = current_offset + str_len;
}

void copy_ndb_value(const NDBTableC* src_table, int src_col, int src_row,
                      NDBTableC* dst_table, int dst_col, int dst_row) {
    if (is_ndb_value_null(src_table, src_col, src_row)) {
        set_ndb_value_null(dst_table, dst_col, dst_row);
        return;
    }
    
    NDBArrayC* src_array = &src_table->columns[src_col];
    NDBArrayC* dst_array = &dst_table->columns[dst_col];
    
    if (src_array->type_id != dst_array->type_id) {
        return; // Type mismatch
    }
    
    if (src_array->type_id == 0) { // int32
        int32_t* src_data = (int32_t*)src_array->values;
        int32_t* dst_data = (int32_t*)dst_array->values;
        dst_data[dst_row] = src_data[src_row];
    } else if (src_array->type_id == 1) { // string
        // Get source string
        char* str_ptr;
        int str_len;
        get_ndb_string_value(src_table, src_col, src_row, &str_ptr, &str_len);
        
        if (str_ptr && str_len > 0) {
            // Improved string copy logic
            char* dst_values = (char*)dst_array->values;
            int32_t* dst_offsets = dst_array->offsets;
            
            // Calculate where the current string should be stored
            int32_t current_offset = 0;
            
            // Traverse previous rows to accumulate offset
            for (int i = 0; i < dst_row; i++) {
                int32_t row_len = dst_offsets[i + 1] - dst_offsets[i];
                current_offset += row_len;
            }
            
            // Copy string data
            memcpy(dst_values + current_offset, str_ptr, str_len);
            
            // Update offsets
            dst_offsets[dst_row] = current_offset;
            dst_offsets[dst_row + 1] = current_offset + str_len;
        } else {
            // Handle empty string
            int32_t* dst_offsets = dst_array->offsets;
            int32_t current_offset = (dst_row > 0) ? dst_offsets[dst_row] : 0;
            dst_offsets[dst_row] = current_offset;
            dst_offsets[dst_row + 1] = current_offset; // Length 0
        }
    }
}

// Create NDB table
NDBTableC* create_ndb_table(int max_rows, int column_count, NDBFieldC* schema) {
    NDBTableC* table = (NDBTableC*)malloc(sizeof(NDBTableC));
    table->num_rows = 0;
    table->num_columns = column_count;
    table->fields = (NDBFieldC*)malloc(column_count * sizeof(NDBFieldC));
    table->columns = (NDBArrayC*)malloc(column_count * sizeof(NDBArrayC));
    
    // Copy schema
    memcpy(table->fields, schema, column_count * sizeof(NDBFieldC));
    
    // Initialize columns
    for (int i = 0; i < column_count; i++) {
        NDBArrayC* array = &table->columns[i];
        array->type_id = schema[i].type_id;
        array->length = max_rows;
        array->null_count = 0;
        
        if (schema[i].nullable) {
            int bitmap_size = (max_rows + 7) / 8;
            array->validity = (uint8_t*)malloc(bitmap_size);
            memset(array->validity, 0xFF, bitmap_size); // Set all to valid
        } else {
            array->validity = NULL;
        }
        
        if (array->type_id == 0) { // int32
            array->values = malloc(max_rows * sizeof(int32_t));
            memset(array->values, 0, max_rows * sizeof(int32_t)); // Initialize to 0
            array->offsets = NULL;
        } else if (array->type_id == 1) { // string
            array->values = malloc(max_rows * 256); // Assume max 256 bytes per string
            memset(array->values, 0, max_rows * 256); // Initialize to 0
            array->offsets = (int32_t*)malloc((max_rows + 1) * sizeof(int32_t));
            
            // Important: initialize all offsets to 0
            for (int j = 0; j <= max_rows; j++) {
                array->offsets[j] = 0;
            }
        }
    }
    
    return table;
}

// Free NDB table
void free_ndb_table(NDBTableC* table) {
    if (!table) return;
    
    for (int i = 0; i < table->num_columns; i++) {
        NDBArrayC* array = &table->columns[i];
        if (array->validity) free(array->validity);
        if (array->values) free(array->values);
        if (array->offsets) free(array->offsets);
    }
    
    free(table->fields);
    free(table->columns);
    free(table);
}

// =================== Vectorization functions ===================

// NDB vectorized key retrieval
void vectorized_get_ndb_keys(const NDBTableC* table, int key_column, int* keys, int start_row, int count) {
    if (!table || !keys || key_column < 0 || key_column >= table->num_columns || 
        start_row < 0 || count <= 0 || start_row + count > table->num_rows) {
        return;
    }
    
    NDBArrayC* array = &table->columns[key_column];
    if (array->type_id != 0) { // Only support int32
        return;
    }
    
    int32_t* column_data = (int32_t*)array->values;
    
    // Loop unrolling optimization
    int full_blocks = count / 4;
    int remainder = count % 4;
    
    for (int i = 0; i < full_blocks; i++) {
        int base_idx = i * 4;
        int src_base = start_row + base_idx;
        keys[base_idx] = column_data[src_base];
        keys[base_idx + 1] = column_data[src_base + 1];
        keys[base_idx + 2] = column_data[src_base + 2];
        keys[base_idx + 3] = column_data[src_base + 3];
    }
    
    int base = full_blocks * 4;
    for (int i = 0; i < remainder; i++) {
        keys[base + i] = column_data[start_row + base + i];
    }
}

// =================== Hash functions ===================

void simple_hash_keys(int* keys, unsigned int* hashes, int count) {
    if (!keys || !hashes || count <= 0) return;
    
    for (int i = 0; i < count; i++) {
        hashes[i] = hash_key(keys[i]);
    }
}

void aligned_hash_keys(int* keys, unsigned int* hashes, int count) {
    if (!keys || !hashes || count <= 0) {
        return;
    }
    
    uintptr_t key_addr = (uintptr_t)keys;
    bool is_aligned = (key_addr % 16 == 0);
    
    if (is_aligned && count >= 32) {
        // For aligned data, use single batch hash
        XXH64_hash_t master_hash = XXH3_64bits(keys, count * sizeof(int));
        
        for (int i = 0; i < count; i++) {
            XXH64_hash_t unique_hash = master_hash ^ 
                                     ((XXH64_hash_t)keys[i] << 16) ^ 
                                     ((XXH64_hash_t)i << 8);
            hashes[i] = (unsigned int)(unique_hash % TABLE_SIZE);
        }
    } else {
        // For non-aligned data, fall back to simple method
        simple_hash_keys(keys, hashes, count);
    }
}


// =================== Callback functions ===================

void standard_ndb_match_processor(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
) {
    int result_row = *result_row_count;
    
    // Copy left table data
    for (int col = 0; col < left_table->num_columns; col++) {
        copy_ndb_value(left_table, col, left_row_idx, 
                        result_table, col, result_row);
    }
    
    // Copy right table data
    for (int col = 0; col < right_table->num_columns; col++) {
        copy_ndb_value(right_table, col, right_row_idx, 
                        result_table, left_table->num_columns + col, result_row);
    }
    
    (*result_row_count)++;
    
    // Update result table row count
    if (result_table->num_rows <= result_row) {
        result_table->num_rows = result_row + 1;
    }
}

void standard_ndb_unmatch_processor(
    const NDBTableC* table, int row_idx,
    NDBTableC* result_table, int* result_row_count,
    int is_left
) {
    int result_row = *result_row_count;
    
    if (is_left) {
        // Copy left table data
        for (int col = 0; col < table->num_columns; col++) {
            copy_ndb_value(table, col, row_idx, 
                            result_table, col, result_row);
        }
        
        // Set right table part to NULL
        int right_start = table->num_columns;
        int right_count = result_table->num_columns - table->num_columns;
        for (int col = 0; col < right_count; col++) {
            int target_col = right_start + col;
            
            // For integer type, set a default value (although it will be overridden by NULL mark)
            if (result_table->columns[target_col].type_id == 0) {
                int32_t* data = (int32_t*)result_table->columns[target_col].values;
                data[result_row] = 0; // Set default value
            }
            // For string type, ensure offsets are correct
            else if (result_table->columns[target_col].type_id == 1) {
                NDBArrayC* array = &result_table->columns[target_col];
                if (array->offsets) {
                    // Ensure string length is 0
                    int32_t current_offset = (result_row > 0) ? array->offsets[result_row] : 0;
                    array->offsets[result_row] = current_offset;
                    array->offsets[result_row + 1] = current_offset; // Length 0
                }
            }
            
            // Set to NULL
            set_ndb_value_null(result_table, target_col, result_row);
        }
    } else {
        // Right table unmatch (RIGHT JOIN, not implemented yet)
        printf("Right table unmatch not implemented\n");
    }
    
    (*result_row_count)++;
    
    // Update result table row count
    if (result_table->num_rows <= result_row) {
        result_table->num_rows = result_row + 1;
    }
}

void selective_ndb_match_processor(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
) {
    int result_row = *result_row_count;
    
    // Only copy the first column (assumed to be key column)
    copy_ndb_value(left_table, 0, left_row_idx, 
                    result_table, 0, result_row);
    
    if (right_table->num_columns > 1) {
        copy_ndb_value(right_table, 1, right_row_idx, 
                        result_table, 1, result_row);
    }
    
    (*result_row_count)++;
    
    // Important: also update result table row count
    if (result_table->num_rows <= result_row) {
        result_table->num_rows = result_row + 1;
    }
}

// =================== Main hash join function ===================

void flexible_ndb_hash_join(
    const NDBTableC* left_table,
    const NDBTableC* right_table,
    int left_key_column,
    int right_key_column,
    JoinType join_type,
    NDBTableC* result_table,
    int* result_row_count,
    ProcessNDBMatchFunc match_processor,
    ProcessNDBUnmatchedFunc unmatch_processor
) {
    HashTable table = {0};
    init_hash_table(&table);
    *result_row_count = 0;
    
    // Build right table hash table
    for (int i = 0; i < right_table->num_rows; i++) {
        int key = get_int_key_from_ndb_column(right_table, right_key_column, i);
        insert_hash(&table, key, i);
    }
    
    // Batch process left table
    const int BATCH_SIZE = 64;
    int* key_batch = malloc(BATCH_SIZE * sizeof(int));
    unsigned int* hash_batch = malloc(BATCH_SIZE * sizeof(unsigned int));
    
    for (int batch_start = 0; batch_start < left_table->num_rows; batch_start += BATCH_SIZE) {
        int batch_size = (batch_start + BATCH_SIZE <= left_table->num_rows) ? 
                        BATCH_SIZE : (left_table->num_rows - batch_start);
        
        // Batch get key values
        vectorized_get_ndb_keys(left_table, left_key_column, key_batch, batch_start, batch_size);
        
        // Batch calculate hash values
        aligned_hash_keys(key_batch, hash_batch, batch_size);
        
        // Process this batch of joins
        for (int i = 0; i < batch_size; i++) {
            int left_row = batch_start + i;
            int key = key_batch[i];
            
            int found_match = 0;
            
            // Find matches
            int hash_idx = lookup_hash(&table, key);
            while (hash_idx != -1) {
                found_match = 1;
                
                // Call match processor
                if (match_processor) {
                    match_processor(left_table, left_row, 
                                  right_table, table.buckets[hash_idx].row_index,
                                  result_table, result_row_count);
                }
                
                // Find next match (handle duplicate keys)
                // Simplified: assume no duplicate keys, actual implementation needs to continue searching
                hash_idx = -1;
            }
            
            // Handle unmatched rows
            if (!found_match && join_type == LEFT_JOIN) {
                if (unmatch_processor) {
                    unmatch_processor(left_table, left_row, 
                                    result_table, result_row_count, 1);
                }
            }
        }
    }
    
    free(key_batch);
    free(hash_batch);
    free_hash_table(&table);
}

// Other missing callback functions
void aggregate_ndb_match_processor(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
) {
    // Simple aggregation: counting
    static int match_count = 0;
    match_count++;
    
    if (*result_row_count == 0) {
        int32_t* count_data = (int32_t*)result_table->columns[0].values;
        count_data[0] = match_count;
        *result_row_count = 1;
        result_table->num_rows = 1;
    } else {
        int32_t* count_data = (int32_t*)result_table->columns[0].values;
        count_data[0] = match_count;
    }
}

void count_ndb_unmatch_processor(
    const NDBTableC* table, int row_idx,
    NDBTableC* result_table, int* result_row_count,
    int is_left
) {
    printf("Unmatched row: %s table row %d\n", is_left ? "left" : "right", row_idx);
}

