#include "mr.h"
#include "kvlist.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void load_input_from_file(kvlist_t *input, const char *filename);
void init_hash_table(void);
void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer, size_t num_reducer, kvlist_t* input, kvlist_t* output);
void execute_reduce_phase(kvlist_t *output);
void print_kvlist(kvlist_t *list);
void cleanup(void);

// Simple mapper function: maps each word to "1"
void simple_mapper(kvpair_t *kv, kvlist_t *output) {
    char *token, *saveptr;
    char *line = strdup(kv->value);  // Make a copy of the value for safe tokenization
    token = strtok_r(line, " ", &saveptr);
    while (token) {
        add_to_hash_table(token);  // Add each word to the hash table
        token = strtok_r(NULL, " ", &saveptr);
    }
    free(line);  // Don't forget to free the duplicated line
}


// Helper function to print kvlist contents for verification
void print_kvlist(kvlist_t *list) {
    kvlist_iterator_t *it = kvlist_iterator_new(list);
    kvpair_t *pair;
    //printf("Output from map_reduce:\n");
    while ((pair = kvlist_iterator_next(it)) != NULL) {
        printf("%s,%s\n", pair->key, pair->value);
    }
    kvlist_iterator_free(&it);
}

// Function to create a predefined input list for testing
kvlist_t *create_test_input() {
    kvlist_t *input = kvlist_new();
    kvlist_append(input, kvpair_new("file1", "Hello world, hello again! Is this the real world, or is it just fantasy Caught in a landslide, no escape from reality."));
    kvlist_append(input, kvpair_new("file2", "hello mapreduce mapreduce"));
    return input;
}

void load_input_from_file(kvlist_t *input, const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Error opening file: %s\n", filename);
        exit(EXIT_FAILURE);
    }

    char *line = NULL;
    size_t len = 0;
    ssize_t read;

    while ((read = getline(&line, &len, file)) != -1) {
        // Strip newline character, if present
        if (line[read - 1] == '\n') {
            line[read - 1] = '\0';
        }
        kvlist_append(input, kvpair_new(filename, strdup(line)));
    }

    free(line);
    fclose(file);
}

// Main function to set up and run tests
int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <file1> [file2] ... [fileN]\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Create input and output lists
    kvlist_t *input = kvlist_new();
    kvlist_t *output = kvlist_new();

    // Load each file specified on the command line into the input list
    for (int i = 1; i < argc; i++) {
        load_input_from_file(input, argv[i]);
    }

    // Initialize the hash table for use in the map phase
    init_hash_table();

    // Execute the MapReduce process
    map_reduce(simple_mapper, argc - 1, NULL, 0, input, output);
    execute_reduce_phase(output);

    // Output the results to verify correct mapping and reducing
    print_kvlist(output);

    // Cleanup resources
    cleanup();
    kvlist_free(&input);
    kvlist_free(&output);

    return 0;
}