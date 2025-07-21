#include "columnar_hashjoin.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 列式存储的示例数据结构
typedef struct {
    ColumnarTable* users;
    ColumnarTable* orders;
    ColumnarTable* payments;
} ColumnarDatabase;

// 创建用户表的列式存储
ColumnarTable* create_users_table() {
    size_t column_sizes[] = {sizeof(int), 32 * sizeof(char)};  // id, name
    ColumnarTable* table = create_columnar_table(100, 2, column_sizes);
    
    // 添加示例数据
    int user_ids[] = {1, 2, 3};
    char user_names[][32] = {"user1_a", "user2_a", "user3_a"};
    
    for (int i = 0; i < 3; i++) {
        add_column_data(table, 0, &user_ids[i], i);
        add_column_data(table, 1, user_names[i], i);
    }
    table->row_count = 3;
    
    return table;
}

// 创建订单表的列式存储
ColumnarTable* create_orders_table() {
    size_t column_sizes[] = {sizeof(int), sizeof(int), 32 * sizeof(char)};  // id, user_id, description
    ColumnarTable* table = create_columnar_table(100, 3, column_sizes);
    
    // 添加示例数据
    int order_ids[] = {101, 102, 103, 104};
    int user_ids[] = {1, 2, 1, 4};
    char order_descs[][32] = {"order1_b", "order2_b", "order3_b", "order4_b"};
    
    for (int i = 0; i < 4; i++) {
        add_column_data(table, 0, &order_ids[i], i);
        add_column_data(table, 1, &user_ids[i], i);
        add_column_data(table, 2, order_descs[i], i);
    }
    table->row_count = 4;
    
    return table;
}

// 创建支付表的列式存储
ColumnarTable* create_payments_table() {
    size_t column_sizes[] = {sizeof(int), sizeof(int), sizeof(float)};  // id, order_id, amount
    ColumnarTable* table = create_columnar_table(100, 3, column_sizes);
    
    // 添加示例数据
    int payment_ids[] = {201, 202};
    int order_ids[] = {101, 102};
    float amounts[] = {99.99f, 149.99f};
    
    for (int i = 0; i < 2; i++) {
        add_column_data(table, 0, &payment_ids[i], i);
        add_column_data(table, 1, &order_ids[i], i);
        add_column_data(table, 2, &amounts[i], i);
    }
    table->row_count = 2;
    
    return table;
}

// 打印列式存储表的内容
void print_columnar_table(const ColumnarTable* table, const char* table_name) {
    printf("\n=== %s ===\n", table_name);
    printf("Rows: %d, Columns: %d\n", table->row_count, table->column_count);
    
    for (int row = 0; row < table->row_count; row++) {
        printf("Row %d: ", row);
        for (int col = 0; col < table->column_count; col++) {
            void* data = get_column_data(table, col, row);
            
            // 根据列的大小判断数据类型（简化处理）
            if (table->column_sizes[col] == sizeof(int)) {
                printf("%d ", *(int*)data);
            } else if (table->column_sizes[col] == sizeof(float)) {
                printf("%.2f ", *(float*)data);
            } else {
                printf("%s ", (char*)data);
            }
        }
        printf("\n");
    }
}

// 打印连接结果
void print_join_result(const ColumnarTable* result, int row_count, const char* description) {
    printf("\n=== %s ===\n", description);
    printf("Result rows: %d\n", row_count);
    
    for (int row = 0; row < row_count; row++) {
        printf("Row %d: ", row);
        
        // 假设结果表结构：user_id, user_name, order_id, user_id_ref, order_desc
        int* user_id = (int*)get_column_data(result, 0, row);
        char* user_name = (char*)get_column_data(result, 1, row);
        int* order_id = (int*)get_column_data(result, 2, row);
        int* user_id_ref = (int*)get_column_data(result, 3, row);
        char* order_desc = (char*)get_column_data(result, 4, row);
        
        printf("user_id=%d, user_name=%s, order_id=%d, user_id_ref=%d, order_desc=%s\n",
               *user_id, user_name, *order_id, *user_id_ref, order_desc);
    }
}

int main() {
    printf("=== 列式存储哈希连接示例 ===\n");
    
    // 创建列式存储的表
    ColumnarTable* users = create_users_table();
    ColumnarTable* orders = create_orders_table();
    ColumnarTable* payments = create_payments_table();
    
    // 打印原始数据
    print_columnar_table(users, "Users Table");
    print_columnar_table(orders, "Orders Table");
    print_columnar_table(payments, "Payments Table");
    
    // 创建结果表：包含users和orders的所有列
    size_t result_column_sizes[] = {
        sizeof(int), 32 * sizeof(char),           // users: id, name
        sizeof(int), sizeof(int), 32 * sizeof(char)  // orders: id, user_id, description
    };
    ColumnarTable* result = create_columnar_table(100, 5, result_column_sizes);
    int result_count = 0;
    
    // 执行 INNER JOIN: users.id = orders.user_id
    printf("\n=== 执行 INNER JOIN ===\n");
    printf("SQL: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id\n");
    
    columnar_hash_join(
        users, orders,
        0, 1,              // users.id (列0) = orders.user_id (列1)
        INNER_JOIN,
        result,
        &result_count
    );
    
    print_join_result(result, result_count, "INNER JOIN Result");
    
    // 重置结果表
    result->row_count = 0;
    result_count = 0;
    
    // 执行 LEFT JOIN
    printf("\n=== 执行 LEFT JOIN ===\n");
    printf("SQL: SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id\n");
    
    columnar_hash_join(
        users, orders,
        0, 1,              // users.id (列0) = orders.user_id (列1)
        LEFT_JOIN,
        result,
        &result_count
    );
    
    print_join_result(result, result_count, "LEFT JOIN Result");
    
    // 清理内存
    free_columnar_table(users);
    free_columnar_table(orders);
    free_columnar_table(payments);
    free_columnar_table(result);
        
    return 0;
}
