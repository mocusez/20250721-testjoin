#include "columnar_hashjoin.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include "xxhash.h"

#define TABLE_SIZE 1024

// 哈希表条目 - 使用开放地址法
typedef struct Entry {
    int row_index;              // 行索引
    int key;                    // 键值
    int is_occupied;            // 槽位是否被占用
    int is_deleted;             // 槽位是否被删除（用于删除操作）
} Entry;

// 哈希表结构
typedef struct {
    Entry buckets[TABLE_SIZE];  // 直接存储条目数组，不使用指针
    int count;                  // 当前存储的元素数量
} HashTable;

// 哈希函数
unsigned int hash_key(int key) {
    return (unsigned int)(XXH3_64bits(&key, sizeof(int)) % TABLE_SIZE);
}

// 线性探测找到下一个位置
int linear_probe(HashTable *table, int key, int start_idx) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        int idx = (start_idx + i) % TABLE_SIZE;
        if (!table->buckets[idx].is_occupied || 
            (table->buckets[idx].is_occupied && table->buckets[idx].key == key)) {
            return idx;
        }
    }
    return -1; // 表满了
}

// 在哈希表中插入行索引
void insert_hash(HashTable *table, int key, int row_index) {
    if (table->count >= TABLE_SIZE * 0.7) {
        // 表快满了，实际使用中应该扩容
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

// 查找哈希表 - 返回匹配条目的索引，-1表示未找到
int lookup_hash(HashTable *table, int key) {
    unsigned int start_idx = hash_key(key);
    
    for (int i = 0; i < TABLE_SIZE; i++) {
        int idx = (start_idx + i) % TABLE_SIZE;
        
        if (!table->buckets[idx].is_occupied && !table->buckets[idx].is_deleted) {
            // 遇到空槽位且未删除，说明键不存在
            return -1;
        }
        
        if (table->buckets[idx].is_occupied && 
            !table->buckets[idx].is_deleted && 
            table->buckets[idx].key == key) {
            return idx;
        }
    }
    
    return -1; // 未找到
}

// 释放哈希表内存（清空所有条目）
void free_hash_table(HashTable *table) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        table->buckets[i].is_occupied = 0;
        table->buckets[i].is_deleted = 0;
        table->buckets[i].key = 0;
        table->buckets[i].row_index = 0;
    }
    table->count = 0;
}

// 初始化哈希表
void init_hash_table(HashTable *table) {
    table->count = 0;
    for (int i = 0; i < TABLE_SIZE; i++) {
        table->buckets[i].is_occupied = 0;
        table->buckets[i].is_deleted = 0;
        table->buckets[i].key = 0;
        table->buckets[i].row_index = 0;
    }
}

// 从列式存储中获取整数键值
int get_int_key_from_column(const ColumnarTable* table, int column_idx, int row_idx) {
    int* column_data = (int*)table->columns[column_idx];
    return column_data[row_idx];
}

// 创建列式存储表
ColumnarTable* create_columnar_table(int max_rows, int column_count, size_t* column_sizes) {
    ColumnarTable* table = malloc(sizeof(ColumnarTable));
    table->row_count = 0;
    table->column_count = column_count;
    table->columns = malloc(column_count * sizeof(void*));
    table->column_sizes = malloc(column_count * sizeof(size_t));
    table->column_names = NULL;
    
    // 为每列分配内存
    for (int i = 0; i < column_count; i++) {
        table->column_sizes[i] = column_sizes[i];
        table->columns[i] = malloc(max_rows * column_sizes[i]);
    }
    
    return table;
}

// 释放列式存储表
void free_columnar_table(ColumnarTable* table) {
    if (!table) return;
    
    for (int i = 0; i < table->column_count; i++) {
        free(table->columns[i]);
    }
    free(table->columns);
    free(table->column_sizes);
    if (table->column_names) {
        for (int i = 0; i < table->column_count; i++) {
            free(table->column_names[i]);
        }
        free(table->column_names);
    }
    free(table);
}

// 向列中添加数据
void add_column_data(ColumnarTable* table, int column_idx, void* data, int row_idx) {
    char* column_base = (char*)table->columns[column_idx];
    size_t element_size = table->column_sizes[column_idx];
    memcpy(column_base + row_idx * element_size, data, element_size);
}

// 从列中获取数据
void* get_column_data(const ColumnarTable* table, int column_idx, int row_idx) {
    char* column_base = (char*)table->columns[column_idx];
    size_t element_size = table->column_sizes[column_idx];
    return column_base + row_idx * element_size;
}

// 复制行数据到结果表
void copy_row_to_result(const ColumnarTable* source_table, int source_row,
                       ColumnarTable* result_table, int result_row,
                       int* column_mapping, int mapping_count) {
    for (int i = 0; i < mapping_count; i++) {
        int source_col = column_mapping[i];
        if (source_col >= 0 && source_col < source_table->column_count) {
            void* source_data = get_column_data(source_table, source_col, source_row);
            add_column_data(result_table, i, source_data, result_row);
        }
    }
}

void columnar_hash_join(
    const ColumnarTable* left_table,
    const ColumnarTable* right_table,
    int left_key_column,
    int right_key_column,
    JoinType join_type,
    ColumnarTable* result_table,
    int* result_row_count
) {
    HashTable table = {0};
    init_hash_table(&table);
    *result_row_count = 0;
    
    // 批量大小，根据缓存行优化
    const int BATCH_SIZE = 64;
    
    // 为右表构建哈希表
    for (int i = 0; i < right_table->row_count; i++) {
        int key = get_int_key_from_column(right_table, right_key_column, i);
        insert_hash(&table, key, i);
    }
    
    // 批量处理左表
    int* key_batch = malloc(BATCH_SIZE * sizeof(int));
    unsigned int* hash_batch = malloc(BATCH_SIZE * sizeof(unsigned int));
    
    for (int batch_start = 0; batch_start < left_table->row_count; batch_start += BATCH_SIZE) {
        int batch_size = (batch_start + BATCH_SIZE <= left_table->row_count) ? 
                        BATCH_SIZE : (left_table->row_count - batch_start);
        
        // 批量获取键值
        vectorized_get_keys(left_table, left_key_column, key_batch, batch_start, batch_size);
        
        // 批量计算哈希值 - 使用最优化版本
        optimized_vectorized_hash_keys(key_batch, hash_batch, batch_size);
        
        // 处理这一批的连接
        for (int i = 0; i < batch_size; i++) {
            int left_row = batch_start + i;
            int key = key_batch[i];
            unsigned int start_idx = hash_batch[i];
            
            int found_match = 0;
            
            // 使用预计算的哈希值进行探测
            for (int probe = 0; probe < TABLE_SIZE; probe++) {
                int idx = (start_idx + probe) % TABLE_SIZE;
                
                if (!table.buckets[idx].is_occupied && !table.buckets[idx].is_deleted) {
                    break;
                }
                
                if (table.buckets[idx].is_occupied && 
                    !table.buckets[idx].is_deleted && 
                    table.buckets[idx].key == key) {
                    
                    found_match = 1;
                    
                    // 复制匹配的行数据
                    int left_col_count = left_table->column_count;
                    int right_col_count = right_table->column_count;
                    
                    for (int col = 0; col < left_col_count; col++) {
                        void* data = get_column_data(left_table, col, left_row);
                        add_column_data(result_table, col, data, *result_row_count);
                    }
                    
                    for (int col = 0; col < right_col_count; col++) {
                        void* data = get_column_data(right_table, col, table.buckets[idx].row_index);
                        add_column_data(result_table, left_col_count + col, data, *result_row_count);
                    }
                    
                    (*result_row_count)++;
                }
            }
            
            // 处理LEFT JOIN的未匹配行
            if (!found_match && join_type == LEFT_JOIN) {
                int left_col_count = left_table->column_count;
                int right_col_count = right_table->column_count;
                
                for (int col = 0; col < left_col_count; col++) {
                    void* data = get_column_data(left_table, col, left_row);
                    add_column_data(result_table, col, data, *result_row_count);
                }
                
                for (int col = 0; col < right_col_count; col++) {
                    size_t element_size = result_table->column_sizes[left_col_count + col];
                    void* null_data = calloc(1, element_size);
                    add_column_data(result_table, left_col_count + col, null_data, *result_row_count);
                    free(null_data);
                }
                
                (*result_row_count)++;
            }
        }
    }
    
    free(key_batch);
    free(hash_batch);
    free_hash_table(&table);
}

// 统一优化的批量哈希函数 - 结合所有优化策略
void optimized_vectorized_hash_keys(int* keys, unsigned int* hashes, int count) {
    if (!keys || !hashes || count <= 0) {
        return;
    }
    
    // 策略1: 超小批量 (1-4) - 直接处理
    if (count <= 4) {
        for (int i = 0; i < count; i++) {
            hashes[i] = (unsigned int)(XXH3_64bits(&keys[i], sizeof(int)) % TABLE_SIZE);
        }
        return;
    }
    
    // 策略2: 小批量 (5-16) - 循环展开
    if (count <= 16) {
        int full_pairs = count / 2;
        for (int i = 0; i < full_pairs; i++) {
            int idx = i * 2;
            hashes[idx] = (unsigned int)(XXH3_64bits(&keys[idx], sizeof(int)) % TABLE_SIZE);
            hashes[idx + 1] = (unsigned int)(XXH3_64bits(&keys[idx + 1], sizeof(int)) % TABLE_SIZE);
        }
        if (count % 2) {
            hashes[count - 1] = (unsigned int)(XXH3_64bits(&keys[count - 1], sizeof(int)) % TABLE_SIZE);
        }
        return;
    }
    
    // 策略3: 中等批量 (17-128) - 批量+种子优化
    if (count <= 128) {
        XXH64_hash_t bulk_hash = XXH3_64bits(keys, count * sizeof(int));
        
        // 优化的种子生成策略
        for (int i = 0; i < count; i++) {
            XXH64_hash_t seed = bulk_hash + ((XXH64_hash_t)i << 32) + keys[i];
            XXH64_hash_t individual_hash = XXH3_64bits_withSeed(&keys[i], sizeof(int), seed);
            hashes[i] = (unsigned int)(individual_hash % TABLE_SIZE);
        }
        return;
    }
    
    // 策略4: 大批量 (129+) - 检查对齐并选择最佳方法
    uintptr_t key_addr = (uintptr_t)keys;
    bool is_aligned = (key_addr % 16 == 0);
    
    if (is_aligned && count >= 256) {
        // 对于大量对齐数据，使用快速位运算方法
        XXH64_hash_t master_hash = XXH3_64bits(keys, count * sizeof(int));
        
        for (int i = 0; i < count; i++) {
            XXH64_hash_t unique_hash = master_hash ^ ((XXH64_hash_t)keys[i] << 16) ^ (XXH64_hash_t)i;
            hashes[i] = (unsigned int)(unique_hash % TABLE_SIZE);
        }
    } else {
        // 对于非对齐或中等大小数据，使用流式API
        XXH3_state_t* state = XXH3_createState();
        if (!state) {
            // 回退到简单方法
            for (int i = 0; i < count; i++) {
                hashes[i] = (unsigned int)(XXH3_64bits(&keys[i], sizeof(int)) % TABLE_SIZE);
            }
            return;
        }
        
        const int CHUNK_SIZE = 64;  // 优化的块大小
        
        for (int chunk_start = 0; chunk_start < count; chunk_start += CHUNK_SIZE) {
            int chunk_size = (chunk_start + CHUNK_SIZE <= count) ? 
                            CHUNK_SIZE : (count - chunk_start);
            
            XXH3_64bits_reset_withSeed(state, (XXH64_hash_t)chunk_start);
            XXH3_64bits_update(state, &keys[chunk_start], chunk_size * sizeof(int));
            XXH64_hash_t base_hash = XXH3_64bits_digest(state);
            
            for (int i = 0; i < chunk_size; i++) {
                int global_idx = chunk_start + i;
                XXH64_hash_t final_hash = base_hash ^ ((XXH64_hash_t)keys[global_idx] << 8) ^ (XXH64_hash_t)i;
                hashes[global_idx] = (unsigned int)(final_hash % TABLE_SIZE);
            }
        }
        
        XXH3_freeState(state);
    }
}

// 简单的逐个哈希函数 - 作为基准
void simple_hash_keys(int* keys, unsigned int* hashes, int count) {
    if (!keys || !hashes || count <= 0) {
        return;
    }
    
    for (int i = 0; i < count; i++) {
        hashes[i] = (unsigned int)(XXH3_64bits(&keys[i], sizeof(int)) % TABLE_SIZE);
    }
}

// 批量哈希函数 - 一次哈希整个数组然后分散
void bulk_hash_keys(int* keys, unsigned int* hashes, int count) {
    if (!keys || !hashes || count <= 0) {
        return;
    }
    
    if (count <= 8) {
        // 小批量直接处理
        simple_hash_keys(keys, hashes, count);
        return;
    }
    
    // 计算整个数组的哈希值
    XXH64_hash_t bulk_hash = XXH3_64bits(keys, count * sizeof(int));
    
    // 为每个键生成不同的哈希值
    for (int i = 0; i < count; i++) {
        XXH64_hash_t seed = bulk_hash + (XXH64_hash_t)i;
        XXH64_hash_t individual_hash = XXH3_64bits_withSeed(&keys[i], sizeof(int), seed);
        hashes[i] = (unsigned int)(individual_hash % TABLE_SIZE);
    }
}

// 对齐优化的哈希函数
void aligned_hash_keys(int* keys, unsigned int* hashes, int count) {
    if (!keys || !hashes || count <= 0) {
        return;
    }
    
    uintptr_t key_addr = (uintptr_t)keys;
    bool is_aligned = (key_addr % 16 == 0);
    
    if (is_aligned && count >= 32) {
        // 对于对齐数据，使用单次批量哈希
        XXH64_hash_t master_hash = XXH3_64bits(keys, count * sizeof(int));
        
        for (int i = 0; i < count; i++) {
            XXH64_hash_t unique_hash = master_hash ^ 
                                     ((XXH64_hash_t)keys[i] << 16) ^ 
                                     ((XXH64_hash_t)i << 8);
            hashes[i] = (unsigned int)(unique_hash % TABLE_SIZE);
        }
    } else {
        // 对于非对齐数据，回退到简单方法
        simple_hash_keys(keys, hashes, count);
    }
}

// 向量化处理函数：批量获取键值
void vectorized_get_keys(const ColumnarTable* table, int key_column, int* keys, int start_row, int count) {
    // 边界检查
    if (!table || !keys || key_column < 0 || key_column >= table->column_count || 
        start_row < 0 || count <= 0 || start_row + count > table->row_count) {
        return;
    }
    
    int* column_data = (int*)table->columns[key_column];
    // 直接使用指针算术，比memcpy更高效
    int* source = column_data + start_row;
    
    // 使用循环展开优化（每次处理4个元素）
    int full_blocks = count / 4;
    int remainder = count % 4;
    
    // 处理完整的4元素块
    for (int i = 0; i < full_blocks; i++) {
        int base_idx = i * 4;
        keys[base_idx] = source[base_idx];
        keys[base_idx + 1] = source[base_idx + 1];
        keys[base_idx + 2] = source[base_idx + 2];
        keys[base_idx + 3] = source[base_idx + 3];
    }
    
    // 处理剩余元素
    int base = full_blocks * 4;
    for (int i = 0; i < remainder; i++) {
        keys[base + i] = source[base + i];
    }
}

