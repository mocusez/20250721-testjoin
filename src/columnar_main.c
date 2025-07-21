#include "columnar_hashjoin.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>

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

void benchmark_hash_functions() {
    const int TEST_SIZES[] = {4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};
    const int NUM_TESTS = sizeof(TEST_SIZES) / sizeof(TEST_SIZES[0]);
    const int ITERATIONS = 10000;  // 增加迭代次数以获得更准确的结果
    
    printf("\n=== Hash Function Performance Benchmark ===\n");
    printf("Testing %d iterations per size\n", ITERATIONS);
    printf("Size\tSimple\t\tBulk\t\tAligned\t\tOptimized\tBest Method\n");
    printf("----\t------\t\t----\t\t-------\t\t---------\t-----------\n");
    
    for (int test = 0; test < NUM_TESTS; test++) {
        int size = TEST_SIZES[test];
        
        // 分配内存并尝试对齐
        int* keys;
        keys = malloc(size * sizeof(int));
        
        unsigned int* hashes1 = malloc(size * sizeof(unsigned int));
        unsigned int* hashes2 = malloc(size * sizeof(unsigned int));
        unsigned int* hashes3 = malloc(size * sizeof(unsigned int));
        unsigned int* hashes4 = malloc(size * sizeof(unsigned int));
        
        // 初始化测试数据 - 使用更真实的数据模式
        srand(42);  // 固定种子确保可重复性
        for (int i = 0; i < size; i++) {
            keys[i] = rand() % 100000 + i;  // 混合随机和顺序模式
        }
        
        // 测试1: Simple Hash (逐个处理)
        clock_t start1 = clock();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            simple_hash_keys(keys, hashes1, size);
        }
        clock_t end1 = clock();
        double time1 = ((double)(end1 - start1)) / CLOCKS_PER_SEC * 1000; // 转换为毫秒
        
        // 测试2: Bulk Hash (批量处理)
        clock_t start2 = clock();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            bulk_hash_keys(keys, hashes2, size);
        }
        clock_t end2 = clock();
        double time2 = ((double)(end2 - start2)) / CLOCKS_PER_SEC * 1000;
        
        // 测试3: Aligned Hash (对齐优化)
        clock_t start3 = clock();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            aligned_hash_keys(keys, hashes3, size);
        }
        clock_t end3 = clock();
        double time3 = ((double)(end3 - start3)) / CLOCKS_PER_SEC * 1000;
        
        // 测试4: Optimized Hash (统一优化)
        clock_t start4 = clock();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            optimized_vectorized_hash_keys(keys, hashes4, size);
        }
        clock_t end4 = clock();
        double time4 = ((double)(end4 - start4)) / CLOCKS_PER_SEC * 1000;
        
        // 验证结果一致性（检查前几个值）
        bool results_match = true;
        int check_count = (size < 5) ? size : 5;
        for (int i = 0; i < check_count; i++) {
            if (hashes1[i] != hashes2[i] || hashes1[i] != hashes3[i] || hashes1[i] != hashes4[i]) {
                results_match = false;
                break;
            }
        }
        
        // 找出最快的方法
        double min_time = time1;
        const char* best = "Simple";
        if (time2 < min_time) { min_time = time2; best = "Bulk"; }
        if (time3 < min_time) { min_time = time3; best = "Aligned"; }
        if (time4 < min_time) { min_time = time4; best = "Optimized"; }
        
        // 计算相对性能（相对于最快方法的倍数）
        printf("%4d\t%6.2fms\t%6.2fms\t%6.2fms\t%6.2fms\t%s", 
               size, time1, time2, time3, time4, best);
        
        if (!results_match) {
            printf(" (WARNING: Results differ!)");
        }
        
        // 显示性能提升
        if (min_time < time1) {
            printf(" (%.1fx faster)", time1 / min_time);
        }
        
        printf("\n");
        
        // 清理内存
        free(keys);
        free(hashes1);
        free(hashes2);
        free(hashes3);
        free(hashes4);
    }
    
    printf("\nNotes:\n");
    printf("- Simple: 逐个处理每个键值\n");
    printf("- Bulk: 批量计算整个数组然后分散\n");
    printf("- Aligned: 检查内存对齐并选择策略\n");
    printf("- Optimized: 根据数据大小智能选择最佳策略\n");
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

    benchmark_hash_functions();
        
    return 0;
}
