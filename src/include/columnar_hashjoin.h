#ifndef COLUMNAR_HASH_JOIN_H
#define COLUMNAR_HASH_JOIN_H

#include <stddef.h>
#include "memory.h"

typedef enum { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN } JoinType;

typedef int (*GetKeyFromNDBColumnFunc)(const NDBTableC* table, int column_idx, int row_idx);

typedef void (*ProcessNDBMatchFunc)(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
);

typedef void (*ProcessNDBUnmatchedFunc)(
    const NDBTableC* table, int row_idx,
    NDBTableC* result_table, int* result_row_count,
    int is_left
);

// Customizable hash join function - using callback functions and NDB format
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
);

// Predefined callback functions - NDB version
void standard_ndb_match_processor(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
);

void selective_ndb_match_processor(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
);

void aggregate_ndb_match_processor(
    const NDBTableC* left_table, int left_row_idx,
    const NDBTableC* right_table, int right_row_idx,
    NDBTableC* result_table, int* result_row_count
);

void standard_ndb_unmatch_processor(
    const NDBTableC* table, int row_idx,
    NDBTableC* result_table, int* result_row_count,
    int is_left
);

void count_ndb_unmatch_processor(
    const NDBTableC* table, int row_idx,
    NDBTableC* result_table, int* result_row_count,
    int is_left
);

// NDB vectorization function declarations
void vectorized_get_ndb_keys(const NDBTableC* table, int key_column, int* keys, int start_row, int count);

// Hash functions with different strategies (unchanged)
void simple_hash_keys(int* keys, unsigned int* hashes, int count);
void aligned_hash_keys(int* keys, unsigned int* hashes, int count);


// NDB utility functions
NDBTableC* create_ndb_table(int max_rows, int column_count, NDBFieldC* schema);
void free_ndb_table(NDBTableC* table);
void add_ndb_column_data(NDBTableC* table, int column_idx, void* data, int row_idx);
void* get_ndb_column_data(const NDBTableC* table, int column_idx, int row_idx);
int get_int_key_from_ndb_column(const NDBTableC* table, int column_idx, int row_idx);

// NDB specific helper functions
int is_ndb_value_null(const NDBTableC* table, int column_idx, int row_idx);
void set_ndb_value_null(NDBTableC* table, int column_idx, int row_idx);
void copy_ndb_value(const NDBTableC* src_table, int src_col, int src_row,
                      NDBTableC* dst_table, int dst_col, int dst_row);

// NDB string processing functions
void get_ndb_string_value(const NDBTableC* table, int column_idx, int row_idx, 
                           char** str_ptr, int* str_len);
void set_ndb_string_value(NDBTableC* table, int column_idx, int row_idx,
                           const char* str, int str_len);

#endif /* COLUMNAR_HASH_JOIN_H */
