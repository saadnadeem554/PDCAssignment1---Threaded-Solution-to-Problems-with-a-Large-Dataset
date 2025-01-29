#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h>
#include <sched.h>
#include <time.h>

int **matrix;
int *row_sums;
int *col_sums;
long long total_sum = 0;
int min_element = INT_MAX;
int max_element = INT_MIN;

int rows, cols;
int num_threads;
int use_affinity;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int start_row;
    int end_row;
    int thread_id;
    // local column sums for each column
    int *local_col_sums;
} ThreadData;

void* compute(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    long long local_sum = 0;
    int local_min = INT_MAX;
    int local_max = INT_MIN;

    if (use_affinity) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int core_id = data->thread_id % 4;
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    //printf("Thread %d bound to core %d\n", data->thread_id, core_id);
}

    for (int i = data->start_row; i < data->end_row; i++) {
        for (int j = 0; j < cols; j++) {
            local_sum += matrix[i][j];
            if (matrix[i][j] < local_min) local_min = matrix[i][j];
            if (matrix[i][j] > local_max) local_max = matrix[i][j];
            row_sums[i] += matrix[i][j];
            data->local_col_sums[j] += matrix[i][j];
        }
    }

    pthread_mutex_lock(&mutex);
    total_sum += local_sum;
    if (local_min < min_element) min_element = local_min;
    if (local_max > max_element) max_element = local_max;
    pthread_mutex_unlock(&mutex);

    pthread_exit(NULL);
}

int main(int argc, char* argv[]) {
    if (argc != 6) {
        printf("Usage: %s <matrix_file> <rows> <cols> <threads> <affinity (0/1)>\n", argv[0]);
        return 1;
    }

    char* filename = argv[1];
    rows = atoi(argv[2]);
    cols = atoi(argv[3]);
    num_threads = atoi(argv[4]);
    use_affinity = atoi(argv[5]);

    matrix = (int**)malloc(rows * sizeof(int*));
    for (int i = 0; i < rows; i++) {
        matrix[i] = (int*)malloc(cols * sizeof(int));
    }
    row_sums = (int*)calloc(rows, sizeof(int));
    col_sums = (int*)calloc(cols, sizeof(int));

    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open file");
        return 1;
    }

    printf("reading data to a matrix\n");
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            fscanf(file, "%d", &matrix[i][j]);
        }
    }
    fclose(file);
    printf("data read successfully\n");

    pthread_t threads[num_threads];
    ThreadData thread_data[num_threads];

    int rows_per_thread = rows / num_threads;
    int remaining_rows = rows % num_threads;

    clock_t start = clock();

    for (int i = 0; i < num_threads; i++) {
        thread_data[i].start_row = i * rows_per_thread;
        thread_data[i].end_row = (i + 1) * rows_per_thread;
        thread_data[i].thread_id = i;
        thread_data[i].local_col_sums = (int*)calloc(cols, sizeof(int));
        if (i == num_threads - 1) {
            thread_data[i].end_row += remaining_rows;
        }
        pthread_create(&threads[i], NULL, compute, (void*)&thread_data[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        // compute column sums
        for (int j = 0; j < cols; j++) {
            col_sums[j] += thread_data[i].local_col_sums[j];
        }
    }

    clock_t end = clock();
    printf("Total Sum: %lld\n", total_sum);
    printf("Min Element: %d\n", min_element);
    printf("Max Element: %d\n", max_element);
/*
    // print row sums
    printf("Row Sums: ");
    for (int i = 0; i < rows; i++) {
        printf("%d \n", row_sums[i]);
    }
    printf("\n");
    // print column sums
    printf("Column Sums: ");
    for (int i = 0; i < cols; i++) {
        printf("%d \n", col_sums[i]);
    }
    printf("\n");
*/
    double time_taken = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Threads execution time: %.2f seconds\n", time_taken);



    free(row_sums);
    free(col_sums);
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);

    return 0;
}