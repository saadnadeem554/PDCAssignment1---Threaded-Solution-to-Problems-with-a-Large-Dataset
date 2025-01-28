#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <time.h>

#define HASH_TABLE_SIZE 10000000
#define MAX_LINE_LENGTH 1024
#define TOP_NODES 10

// Hash map for node degrees
typedef struct HashEntry {
    int node;
    int degree;
    struct HashEntry *next;
} HashEntry;

HashEntry *hash_table[HASH_TABLE_SIZE];
pthread_mutex_t hash_locks[HASH_TABLE_SIZE]; // Locks for hash buckets
long total_edges = 0;
long total_nodes = 0;
pthread_mutex_t total_edge_lock = PTHREAD_MUTEX_INITIALIZER; // Lock for edges
pthread_mutex_t total_node_lock = PTHREAD_MUTEX_INITIALIZER; // Lock for nodes

typedef struct TopNode {
    int node;
    int degree;
} TopNode;

TopNode top_nodes[TOP_NODES]; // Array for top nodes
pthread_mutex_t top_nodes_lock = PTHREAD_MUTEX_INITIALIZER; // Lock for top_nodes array

int with_affinity = 0; // Flag for enabling affinity

// Hash function
int hash_function(int node) {
    return node % HASH_TABLE_SIZE;
}

// Update top nodes array
void update_top_nodes_array(int node, int degree) {
    pthread_mutex_lock(&top_nodes_lock);

    for (int i = 0; i < TOP_NODES; i++) {
        if (top_nodes[i].node == node) {
            top_nodes[i].degree = degree;
            pthread_mutex_unlock(&top_nodes_lock);
            return;
        }
    }

    int min_index = 0;
    for (int i = 1; i < TOP_NODES; i++) {
        if (top_nodes[i].degree < top_nodes[min_index].degree) {
            min_index = i;
        }
    }

    if (degree > top_nodes[min_index].degree) {
        top_nodes[min_index].node = node;
        top_nodes[min_index].degree = degree;
    }

    pthread_mutex_unlock(&top_nodes_lock);
}

// Update node degree
void update_node_degree(int node) {
    int hash_index = hash_function(node);
    pthread_mutex_lock(&hash_locks[hash_index]);

    HashEntry *entry = hash_table[hash_index];
    while (entry != NULL) {
        if (entry->node == node) {
            entry->degree++;
            pthread_mutex_unlock(&hash_locks[hash_index]);
            update_top_nodes_array(node, entry->degree);
            return;
        }
        entry = entry->next;
    }

    entry = (HashEntry *)malloc(sizeof(HashEntry));
    entry->node = node;
    entry->degree = 1;
    entry->next = hash_table[hash_index];
    hash_table[hash_index] = entry;

    pthread_mutex_unlock(&hash_locks[hash_index]);
    update_top_nodes_array(node, entry->degree);
}

// Increment edge count
void increment_edge_count() {
    pthread_mutex_lock(&total_edge_lock);
    total_edges++;
    pthread_mutex_unlock(&total_edge_lock);
}

// Print top nodes
void print_top_nodes() {
    pthread_mutex_lock(&top_nodes_lock);

    printf("Top %d nodes with highest degrees:\n", TOP_NODES);
    for (int i = 0; i < TOP_NODES - 1; i++) {
        for (int j = i + 1; j < TOP_NODES; j++) {
            if (top_nodes[i].degree < top_nodes[j].degree) {
                TopNode temp = top_nodes[i];
                top_nodes[i] = top_nodes[j];
                top_nodes[j] = temp;
            }
        }
    }

    for (int i = 0; i < TOP_NODES; i++) {
        if (top_nodes[i].degree > 0) {
            printf("Node: %d, Degree: %d\n", top_nodes[i].node, top_nodes[i].degree);
        }
    }

    pthread_mutex_unlock(&top_nodes_lock);
}

// Free hash table
void free_hash_table() {
    for (int i = 0; i < HASH_TABLE_SIZE; i++) {
        pthread_mutex_lock(&hash_locks[i]);
        HashEntry *entry = hash_table[i];
        while (entry != NULL) {
            HashEntry *temp = entry;
            entry = entry->next;
            free(temp);
        }
        pthread_mutex_unlock(&hash_locks[i]);
    }
}

// Set thread affinity
void set_thread_affinity(pthread_t thread, int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    int result = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        fprintf(stderr, "Error setting thread affinity: %s\n", strerror(result));
    } else {
        printf("Thread %lu bound to core %d\n", thread, core_id);
    }
}

// Process lines
void *process_lines(void *arg) {
    char **args = (char **)arg;
    char *filename = args[0];
    long start_line = atol(args[1]);
    long end_line = atol(args[2]);
    int thread_id = atoi(args[3]);

    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return NULL;
    }

    if (with_affinity) {
        set_thread_affinity(pthread_self(), thread_id % sysconf(_SC_NPROCESSORS_ONLN));
    }

    char line[MAX_LINE_LENGTH];
    long line_number = 0;

    fseek(file, 0, SEEK_SET);
    while (line_number < start_line && fgets(line, MAX_LINE_LENGTH, file)) {
        line_number++;
    }

    while (line_number < end_line && fgets(line, MAX_LINE_LENGTH, file)) {
        if (line[0] != '#') {
            int from, to;
            if (sscanf(line, "%d %d", &from, &to) == 2) {
                increment_edge_count();
                update_node_degree(from);
                update_node_degree(to);
            }
        }
        line_number++;
    }

    fclose(file);
    return NULL;
}
// Count total nodes in the hash table
void count_total_nodes() {
    for (int i = 0; i < HASH_TABLE_SIZE; i++) {
        pthread_mutex_lock(&hash_locks[i]);
        HashEntry *entry = hash_table[i];
        while (entry != NULL) {
            total_nodes++;
            entry = entry->next;
        }
        pthread_mutex_unlock(&hash_locks[i]);
    }
}
int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <filename> <thread_count> <with_affinity>\n", argv[0]);
        return 1;
    }

    char *filename = argv[1];
    int thread_count = atoi(argv[2]);
    with_affinity = atoi(argv[3]);

    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return 1;
    }

    long total_lines = 0;
    char line[MAX_LINE_LENGTH];
    while (fgets(line, MAX_LINE_LENGTH, file)) {
        total_lines++;
    }
    fclose(file);

    long lines_per_thread = total_lines / thread_count;
    pthread_t threads[thread_count];

    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);


    for (int i = 0; i < thread_count; i++) {
        long start_line = i * lines_per_thread;
        long end_line = (i == thread_count - 1) ? total_lines : (i + 1) * lines_per_thread;

        char **args = malloc(4 * sizeof(char *));
        args[0] = filename;
        args[1] = malloc(20);
        args[2] = malloc(20);
        args[3] = malloc(20);
        sprintf(args[1], "%ld", start_line);
        sprintf(args[2], "%ld", end_line);
        sprintf(args[3], "%d", i);

        pthread_create(&threads[i], NULL, process_lines, args);
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    count_total_nodes();
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    long seconds = end_time.tv_sec - start_time.tv_sec;
    long nanoseconds = end_time.tv_nsec - start_time.tv_nsec;
    double elapsed_time = seconds + nanoseconds * 1e-9;

    printf("Total Nodes: %ld\n", total_nodes);
    printf("Total Edges: %ld\n", total_edges);
    printf("Execution Time: %.6f seconds\n", elapsed_time);

    print_top_nodes();
    free_hash_table();

    return 0;
}
