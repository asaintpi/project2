#include "mr.h"
#include "kvlist.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    printf("Output from map_reduce:\n");
    while ((pair = kvlist_iterator_next(it)) != NULL) {
        printf("%s: %s\n", pair->key, pair->value);
    }
    kvlist_iterator_free(&it);
}

// Function to create a predefined input list for testing
kvlist_t *create_test_input() {
    kvlist_t *input = kvlist_new();
    kvlist_append(input, kvpair_new("file1", "hello world hello"));
    kvlist_append(input, kvpair_new("file2", "hello mapreduce mapreduce"));
    return input;
}

// Main function to set up and run tests
int main() {
    // Create input and output lists
    kvlist_t *input = create_test_input();
    kvlist_t *output = kvlist_new();

    // Call the map_reduce function
    init_hash_table();  // Initialize hash table

    map_reduce(simple_mapper, 2, NULL, 0, input, output); // Reducer is NULL for now
    print_hash_table(); // Print results from hash table

    // Print output to verify correct mapping
    print_kvlist(output);

    // Cleanup
    kvlist_free(&input);
    kvlist_free(&output);
    return 0;
}