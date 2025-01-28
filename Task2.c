#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h>

int **matrix;
int *row_sums;
int *col_sums;
long long total_sum = 0;
int min_element = INT_MAX;
int max_element = INT_MIN;

int rows, cols;
int num_threads;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int start_row;
    int end_row;
} ThreadData;

void* compute(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    long long local_sum = 0;
    int local_min = INT_MAX;
    int local_max = INT_MIN;

    for (int i = data->start_row; i < data->end_row; i++) {
        for (int j = 0; j < cols; j++) {
            local_sum += matrix[i][j];
            if (matrix[i][j] < local_min) local_min = matrix[i][j];
            if (matrix[i][j] > local_max) local_max = matrix[i][j];
            row_sums[i] += matrix[i][j];
            col_sums[j] += matrix[i][j];
        }
    }

    // Update global values with thread-local results
    pthread_mutex_lock(&mutex);
    total_sum += local_sum;
    if (local_min < min_element) min_element = local_min;
    if (local_max > max_element) max_element = local_max;
    pthread_mutex_unlock(&mutex);

    pthread_exit(NULL);
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s <matrix_file> <rows> <cols>\n", argv[0]);
        return 1;
    }

    char* filename = argv[1];
    rows = atoi(argv[2]);
    cols = atoi(argv[3]);

    // Allocate memory for matrix, row sums, and column sums
    matrix = (int**)malloc(rows * sizeof(int*));
    for (int i = 0; i < rows; i++) {
        matrix[i] = (int*)malloc(cols * sizeof(int));
    }
    row_sums = (int*)calloc(rows, sizeof(int));
    col_sums = (int*)calloc(cols, sizeof(int));

    // Load matrix from file
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open file");
        return 1;
    }

    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            fscanf(file, "%d", &matrix[i][j]);
        }
    }
    fclose(file);

    printf("Enter number of threads: ");
    scanf("%d", &num_threads);

    pthread_t threads[num_threads];
    ThreadData thread_data[num_threads];

    int rows_per_thread = rows / num_threads;
    int remaining_rows = rows % num_threads;

    clock_t start = clock();

    // Create threads
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].start_row = i * rows_per_thread;
        thread_data[i].end_row = (i + 1) * rows_per_thread;
        if (i == num_threads - 1) {
            thread_data[i].end_row += remaining_rows; // Assign remaining rows to the last thread
        }
        pthread_create(&threads[i], NULL, compute, (void*)&thread_data[i]);
    }

    // Join threads
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_t end = clock();

    // Print results
    printf("Total Sum: %lld\n", total_sum);
    printf("Min Element: %d\n", min_element);
    printf("Max Element: %d\n", max_element);
    double time_taken = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Threads execution time: %.2f seconds\n", time_taken);
    /*
    for(int i = 0; i < rows; i++) {
        printf("Row %d Sum: %d\n", i, row_sums[i]);
    }
    for(int i = 0; i < cols; i++) {
        printf("Column %d Sum: %d\n", i, col_sums[i]);
    }
    // Free allocated memory
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    */
    free(matrix);
    free(row_sums);
    free(col_sums);

    return 0;
}