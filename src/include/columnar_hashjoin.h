#ifndef COLUMNAR_HASH_JOIN_H
#define COLUMNAR_HASH_JOIN_H

#include <stddef.h>

// Join Types
typedef enum { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN } JoinType;

// 列式存储表结构
typedef struct {
    int row_count;              // 行数
    int column_count;           // 列数
    void** columns;             // 列数组，每个元素指向一列的数据
    size_t* column_sizes;       // 每列元素的大小
    char** column_names;        // 列名（可选）
} ColumnarTable;

// 列式存储的键提取函数 - 传入列索引和行索引
typedef int (*GetKeyFromColumnFunc)(const ColumnarTable* table, int column_idx, int row_idx);

// 列式存储的结果处理函数
typedef void (*ProcessColumnarMatchFunc)(
    const ColumnarTable* left_table, int left_row_idx,
    const ColumnarTable* right_table, int right_row_idx,
    ColumnarTable* result_table, int* result_row_count
);

typedef void (*ProcessColumnarUnmatchedFunc)(
    const ColumnarTable* table, int row_idx,
    ColumnarTable* result_table, int* result_row_count,
    int is_left
);

// 列式存储的哈希连接函数
void columnar_hash_join(
    const ColumnarTable* left_table,
    const ColumnarTable* right_table,
    int left_key_column,        // 左表键列索引
    int right_key_column,       // 右表键列索引
    JoinType join_type,
    ColumnarTable* result_table,
    int* result_row_count
);

// 工具函数
ColumnarTable* create_columnar_table(int max_rows, int column_count, size_t* column_sizes);
void free_columnar_table(ColumnarTable* table);
void add_column_data(ColumnarTable* table, int column_idx, void* data, int row_idx);
void* get_column_data(const ColumnarTable* table, int column_idx, int row_idx);

#endif /* COLUMNAR_HASH_JOIN_H */
