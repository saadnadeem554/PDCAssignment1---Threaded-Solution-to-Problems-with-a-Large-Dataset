#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h>
#include <string.h>

#define THREAD_COUNT 4 // Number of threads

// Global variables for the final results
long long total_sum = 0;
long long *column_sums;
int total_rows = 0;
int total_cols = 0;
int *row_sums;
int global_max = INT_MIN;
int global_min = INT_MAX;

// Mutex for protecting shared data
pthread_mutex_t sum_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t max_min_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    char *filename;
    int start_row;
    int end_row;
    // array to store the local column sums for each thread
    long long *local_col_sums;  // Local column sums for each thread
} ThreadData;

// Function to check if a string is a "na", "n", or "nan"
int is_invalid_value(const char *token) {
    return (strcmp(token, "na") == 0 || strcmp(token, "n") == 0 || strcmp(token, "nan") == 0);
}

// Thread function
void *process_chunk(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    FILE *file = fopen(data->filename, "r");
    if (!file) {
        perror("Error opening file");
        pthread_exit(NULL);
    }

    data->local_col_sums = calloc(total_cols, sizeof(long long));  // Local column sums for each thread
    char line[1024 * 8];
    long long local_sum = 0;
    int local_max = INT_MIN;
    int local_min = INT_MAX;

    int row_idx = 0;
    // Skip rows before start_row
    while (row_idx < data->start_row && fgets(line, sizeof(line), file)) {
        row_idx++;
    }

    // Process assigned rows
        for (int i = data->start_row; i < data->end_row; i++) {
            if (!fgets(line, sizeof(line), file)) break;  // Read the next line

            int row_sum = 0;  // Sum of values in the current row
            char *token = strtok(line, " ");  // Get the first token (column value)

            for (int col = 0; col < total_cols; col++) {
                int value = 0;

                // Check if the token is valid
                if (token) {
                    value = atoi(token);  // Convert token to integer
                } else {
                    value = 0;  // Default to 0 if token is missing
                }

                row_sum += value;  // Add to the row sum
                data->local_col_sums[col] += value;  // Update column sum
                local_sum += value;  // Update total sum

                // Update min and max values
                if (value > local_max) local_max = value;
                if (value < local_min && value != 0) local_min = value;

                // Move to the next token
                token = strtok(NULL, " ");
            }

            row_sums[i] = row_sum;  // Store the row sum
        }


    fclose(file);

    // Update global results
    pthread_mutex_lock(&sum_lock);
    total_sum += local_sum;
    pthread_mutex_unlock(&sum_lock);

    // Update global max and min with mutex
    pthread_mutex_lock(&max_min_lock);
    if (local_max > global_max) global_max = local_max;
    if (local_min < global_min) global_min = local_min;
    pthread_mutex_unlock(&max_min_lock);

    pthread_exit(NULL);
}

void count_rows_and_columns(char *filename, int *row_count, int *col_count) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return;
    }

    char *line = NULL;
    size_t len = 0;
    *row_count = 0;
    *col_count = 0;

    // Count rows and columns, and ensure all rows have consistent column counts
int consistent_columns = 1;  // Flag to track consistency
int first_row_col_count = -1; // Store column count of the first row for comparison

while (getline(&line, &len, file) != -1) {
    (*row_count)++;  // Increment row count

    // Count columns in the current row
    int col = 0;
    char *token = strtok(line, " ");
    while (token) {
        col++;
        token = strtok(NULL, " ");
    }

    // On the first row, store the column count for consistency checking
    if (first_row_col_count == -1) {
        first_row_col_count = col;
    } else if (col != first_row_col_count) {
        consistent_columns = 0;  // Mark inconsistency if column count doesn't match
    }

    // Track the maximum column count
    if (*col_count < col) {
        *col_count = col;
    }
}

// Print a warning if column counts are inconsistent
if (!consistent_columns) {
    printf("Warning: Rows have inconsistent numbers of columns.\n");
} else {
    printf("All rows have a consistent number of columns (%d).\n", first_row_col_count);
}

    fclose(file);
    free(line);
    printf("Rows: %d, Columns: %d\n", *row_count, *col_count);
    total_rows = *row_count;
    total_cols = *col_count;
}

int main() {
    char *filename = "matrix.txt";
    int row_count = 0, col_count = 0;

    // Step 1: Count rows and columns
    count_rows_and_columns(filename, &row_count, &col_count);

    // Allocate memory for row sums and column sums
    row_sums = calloc(row_count, sizeof(int));
    column_sums = calloc(col_count, sizeof(long long));
    if (!row_sums || !column_sums) {
        perror("Memory allocation failed");
        return 1;
    }

    // Calculate rows per thread
    int rows_per_thread = row_count / THREAD_COUNT;

    // Create threads
    pthread_t threads[THREAD_COUNT];
    ThreadData thread_data[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        thread_data[i].filename = filename;
        thread_data[i].start_row = i * rows_per_thread;
        thread_data[i].end_row = (i == THREAD_COUNT - 1) ? row_count : (i + 1) * rows_per_thread;
        pthread_create(&threads[i], NULL, process_chunk, &thread_data[i]);
    }

    // Wait for threads to complete
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);

        // After each thread completes, add its local column sums to the global column sums
        for (int j = 0; j < total_cols; j++) {
            column_sums[j] += thread_data[i].local_col_sums[j];
            //if(j==8599)
            //printf("column_sums[%d] = %lld\n", j, column_sums[j]);
        }

        free(thread_data[i].local_col_sums);  // Free local column sums for this thread
    }

    // Print results
    printf("Total Sum: %lld\n", total_sum);
    printf("Maximum Element: %d\n", global_max);
    printf("Minimum Element: %d\n", global_min);
    //printf("column_sums[%d] = %lld\n", 0, column_sums[5]);



    // Print row sums
    printf("Row-wise sums:\n");
    for (int i = 0; i < row_count; i++) {
        printf("Row %d: %d\n", i, row_sums[i]);
    }
    // Print column sums
    printf("Column-wise sums:\n");
    for (int i = 0; i < col_count; i++) {
        if(column_sums[i] != 0)
        printf("Column %d: %lld\n", i, column_sums[i]);
    }

    
    // Free allocated memory
    free(row_sums);
    free(column_sums);

    return 0;
}
