#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "memory.h"
#include "columnar_hashjoin.h"

void print_table(const NDBTableC* table) {
    for (int row = 0; row < table->num_rows; row++) {
        for (int col = 0; col < table->num_columns; col++) {
            NDBArrayC* array = &table->columns[col];
            NDBFieldC* field = &table->fields[col];
            
            if (array->validity) {
                int byte_idx = row / 8;
                int bit_idx = row % 8;
                if (!(array->validity[byte_idx] & (1 << bit_idx))) {
                    printf("%s: NULL\t", field->name);
                    continue;
                }
            }
            
            if (array->type_id == 0) {  // int32
                int32_t* data = (int32_t*)array->values;
                printf("%s: %d\t", field->name, data[row]);
            } else if (array->type_id == 1) {  // string
                int32_t start = array->offsets[row];
                int32_t end = array->offsets[row + 1];
                char* str = (char*)array->values + start;
                printf("%s: %.*s\t", field->name, end - start, str);
            }
        }
        printf("\n");
    }
}

// Create left table - Employee table
NDBTableC* create_employee_table() {
    NDBFieldC schema[2] = {
        {.name = "emp_id", .type_id = 0, .nullable = 0},     // int32, not null
        {.name = "emp_name", .type_id = 1, .nullable = 0}    // string, not null
    };

    // Create table
    NDBTableC* table = create_ndb_table(5, 2, schema);
    
    // Employee data: {1, "Alice"}, {2, "Bob"}, {3, "Charlie"}, {4, "David"}
    int32_t emp_ids[] = {1, 2, 3, 4};
    char emp_names[] = "AliceBobCharleDavid";  // Continuous storage: "Alice" + "Bob" + "Charle" + "David"
    int32_t emp_offsets[] = {0, 5, 8, 14, 19}; // Alice(0-5), Bob(5-8), Charle(8-14), David(14-19)
    
    // Set emp_id column
    int32_t* id_data = (int32_t*)table->columns[0].values;
    for (int i = 0; i < 4; i++) {
        id_data[i] = emp_ids[i];
    }
    
    // Set emp_name column
    memcpy(table->columns[1].values, emp_names, sizeof(emp_names));
    memcpy(table->columns[1].offsets, emp_offsets, sizeof(emp_offsets));
    
    table->num_rows = 4;
    return table;
}

// Create right table - Department table
NDBTableC* create_department_table() {
    NDBFieldC schema[2] = {
        {.name = "emp_id", .type_id = 0, .nullable = 0},     // int32, not null
        {.name = "dept_name", .type_id = 1, .nullable = 0}   // string, not null
    };

    // Create table
    NDBTableC* table = create_ndb_table(5, 2, schema);
    
    // Department data: {2, "Engineering"}, {3, "Marketing"}, {4, "Sales"}, {5, "HR"}
    int32_t emp_ids[] = {2, 3, 4, 5};
    char dept_names[] = "EngineeringMarketingSalesHR";  // Continuous storage
    int32_t dept_offsets[] = {0, 11, 20, 25, 27}; // Engineering(0-11), Marketing(11-20), Sales(20-25), HR(25-27)
    
    // Set emp_id column
    int32_t* id_data = (int32_t*)table->columns[0].values;
    for (int i = 0; i < 4; i++) {
        id_data[i] = emp_ids[i];
    }
    
    // Set dept_name column
    memcpy(table->columns[1].values, dept_names, sizeof(dept_names));
    memcpy(table->columns[1].offsets, dept_offsets, sizeof(dept_offsets));
    
    table->num_rows = 4;
    return table;
}

// Create result table for storing join results
NDBTableC* create_result_table() {
    NDBFieldC schema[4] = {
        {.name = "emp_id", .type_id = 0, .nullable = 0},       // int32, not null
        {.name = "emp_name", .type_id = 1, .nullable = 0},     // string, not null
        {.name = "emp_id_right", .type_id = 0, .nullable = 1}, // int32, nullable (for LEFT JOIN)
        {.name = "dept_name", .type_id = 1, .nullable = 1}     // string, nullable (for LEFT JOIN)
    };

    return create_ndb_table(10, 4, schema);
}

void print_table_debug(const NDBTableC* table, const char* table_name) {
    printf("=== Debug info: %s ===\n", table_name);
    printf("Table pointer: %p\n", (void*)table);
    if (!table) {
        printf("Table is NULL!\n");
        return;
    }
    
    printf("Rows: %d, Columns: %d\n", table->num_rows, table->num_columns);
    printf("Fields pointer: %p, Columns pointer: %p\n", (void*)table->fields, (void*)table->columns);
    
    for (int col = 0; col < table->num_columns; col++) {
        NDBFieldC* field = &table->fields[col];
        NDBArrayC* array = &table->columns[col];
        printf("Column %d: name=%s, type=%d, values pointer=%p\n", 
               col, field->name, field->type_id, array->values);
    }
    
    printf("=== Table content ===\n");
    for (int row = 0; row < table->num_rows; row++) {
        printf("Row %d: ", row);
        for (int col = 0; col < table->num_columns; col++) {
            NDBArrayC* array = &table->columns[col];
            NDBFieldC* field = &table->fields[col];
            
            // Check NULL values
            if (array->validity && !((array->validity[row / 8] >> (row % 8)) & 1)) {
                printf("%s: NULL\t", field->name);
                continue;
            }
            
            if (array->type_id == 0) {  // int32
                if (!array->values) {
                    printf("%s: <NULL_PTR>\t", field->name);
                    continue;
                }
                int32_t* data = (int32_t*)array->values;
                printf("%s: %d\t", field->name, data[row]);
            } else if (array->type_id == 1) {  // string
                if (!array->values || !array->offsets) {
                    printf("%s: <NULL_PTR>\t", field->name);
                    continue;
                }
                int32_t start = array->offsets[row];
                int32_t end = array->offsets[row + 1];
                char* str = (char*)array->values + start;
                printf("%s: %.*s\t", field->name, end - start, str);
            }
        }
        printf("\n");
    }
    printf("=== End ===\n\n");
}

int main() {
    printf("\n=== NDB Hash Join Example ===\n\n");

    // Create employee table and department table
    NDBTableC* emp_table = create_employee_table();
    NDBTableC* dept_table = create_department_table();
    
    printf("Employee table (left table):\n");
    print_table(emp_table);
    
    printf("\nDepartment table (right table):\n");
    print_table(dept_table);

    // Create result table
    NDBTableC* result_table = create_result_table();
    int result_row_count = 0;

    printf("\n--- INNER JOIN Result ---\n");
    
    // Execute INNER JOIN
    flexible_ndb_hash_join(
        emp_table,           // Left table (employee table)
        dept_table,          // Right table (department table)
        0,                   // Left table join key column index (emp_id)
        0,                   // Right table join key column index (emp_id)
        INNER_JOIN,          // Join type
        result_table,        // Result table
        &result_row_count,   // Result row count
        standard_ndb_match_processor,    // Match processor
        standard_ndb_unmatch_processor   // Unmatch processor
    );
    
    printf("INNER JOIN result (emp_id = emp_id):\n");
    printf("Matched rows: %d\n", result_row_count);
    
    // Use original print_table function
    print_table(result_table);

    // Reset result table for LEFT JOIN
    printf("\n--- LEFT JOIN Result ---\n");
    result_row_count = 0;
    
    // Recreate result table
    free_ndb_table(result_table);
    result_table = create_result_table();
    
    flexible_ndb_hash_join(
        emp_table,           // Left table (employee table)
        dept_table,          // Right table (department table)
        0,                   // Left table join key column index (emp_id)
        0,                   // Right table join key column index (emp_id)
        LEFT_JOIN,           // Join type
        result_table,        // Result table
        &result_row_count,   // Result row count
        standard_ndb_match_processor,    // Match processor
        standard_ndb_unmatch_processor   // Unmatch processor
    );
    
    printf("LEFT JOIN result (emp_id = emp_id):\n");
    printf("Total rows: %d\n", result_row_count);
    
    // Debug LEFT JOIN result
    // print_table_debug(result_table, "LEFT JOIN result table");
    print_table(result_table);

    // Demonstrate selective join
    printf("\n--- Selective join (only select specific columns) ---\n");
    result_row_count = 0;
    
    // Recreate simplified result table (only 2 columns)
    free_ndb_table(result_table);
    NDBFieldC simple_schema[2] = {
        {.name = "emp_id", .type_id = 0, .nullable = 0},
        {.name = "dept_name", .type_id = 1, .nullable = 0}
    };
    result_table = create_ndb_table(10, 2, simple_schema);
    
    flexible_ndb_hash_join(
        emp_table,           // Left table (employee table)
        dept_table,          // Right table (department table)
        0,                   // Left table join key column index (emp_id)
        0,                   // Right table join key column index (emp_id)
        INNER_JOIN,          // Join type
        result_table,        // Result table
        &result_row_count,   // Result row count
        selective_ndb_match_processor,   // Selective match processor
        NULL                               // Don't handle unmatched
    );
    
    printf("Selective join result (only show emp_id and dept_name):\n");
    printf("Rows: %d\n", result_row_count);
    
    // Debug selective join result
    // print_table_debug(result_table, "selective join result table");
    print_table(result_table);

    // Clean up memory
    free_ndb_table(emp_table);
    free_ndb_table(dept_table);
    free_ndb_table(result_table);

    printf("\n=== Hash join example completed ===\n");

    return 0;
}