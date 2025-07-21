#include "columnar_hashjoin.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

// 列式存储的哈希连接实现
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
    *result_row_count = 0;
    
    // 为右表构建哈希表 - 只存储行索引
    for (int i = 0; i < right_table->row_count; i++) {
        int key = get_int_key_from_column(right_table, right_key_column, i);
        insert_hash(&table, key, i);
    }
    
    // 遍历左表进行连接
    for (int left_row = 0; left_row < left_table->row_count; left_row++) {
        int key = get_int_key_from_column(left_table, left_key_column, left_row);
        
        int found_match = 0;
        
        // 查找所有匹配的右表行（处理重复键的情况）
        unsigned int start_idx = hash_key(key);
        for (int probe = 0; probe < TABLE_SIZE; probe++) {
            int idx = (start_idx + probe) % TABLE_SIZE;
            
            if (!table.buckets[idx].is_occupied && !table.buckets[idx].is_deleted) {
                // 遇到空槽位且未删除，停止搜索
                break;
            }
            
            if (table.buckets[idx].is_occupied && 
                !table.buckets[idx].is_deleted && 
                table.buckets[idx].key == key) {
                
                found_match = 1;
                
                // 将匹配的行数据复制到结果表
                int left_col_count = left_table->column_count;
                int right_col_count = right_table->column_count;
                
                // 复制左表数据
                for (int col = 0; col < left_col_count; col++) {
                    void* data = get_column_data(left_table, col, left_row);
                    add_column_data(result_table, col, data, *result_row_count);
                }
                
                // 复制右表数据
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
            
            // 复制左表数据
            for (int col = 0; col < left_col_count; col++) {
                void* data = get_column_data(left_table, col, left_row);
                add_column_data(result_table, col, data, *result_row_count);
            }
            
            // 右表部分填充NULL值（这里用零值代替）
            for (int col = 0; col < right_col_count; col++) {
                size_t element_size = result_table->column_sizes[left_col_count + col];
                void* null_data = calloc(1, element_size);  // 零值
                add_column_data(result_table, left_col_count + col, null_data, *result_row_count);
                free(null_data);
            }
            
            (*result_row_count)++;
        }
    }
    
    free_hash_table(&table);
}

// 向量化处理函数示例：批量获取键值
void vectorized_get_keys(const ColumnarTable* table, int key_column, int* keys, int start_row, int count) {
    int* column_data = (int*)table->columns[key_column];
    memcpy(keys, column_data + start_row, count * sizeof(int));
}

// 向量化哈希函数：批量计算哈希值
void vectorized_hash_keys(int* keys, unsigned int* hashes, int count) {
    for (int i = 0; i < count; i++) {
        hashes[i] = (unsigned int)(XXH3_64bits(&keys[i], sizeof(int)) % TABLE_SIZE);
    }
}
