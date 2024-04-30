#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mr.h"
#include "kvlist.h"

// Simple mapper function: each word mapped to "1"
void test_mapper(kvpair_t *kv, kvlist_t *output) {
    char *token, *saveptr;
    token = strtok_r(kv->value, " ", &saveptr);
    while (token) {
        kvpair_t *new_pair = kvpair_new(token, "1");
        kvlist_append(output, new_pair);
        token = strtok_r(NULL, " ", &saveptr);
    }
}

// Simple reducer function: summing values
void test_reducer(char *key, kvlist_t *values, kvlist_t *output) {
    int sum = 0;
    kvlist_iterator_t *iter = kvlist_iterator_new(values);
    kvpair_t *pair;
    while ((pair = kvlist_iterator_next(iter)) != NULL) {
        sum += atoi(pair->value);
    }
    kvlist_iterator_free(&iter);
    char sum_str[32];
    sprintf(sum_str, "%d", sum);
    kvlist_append(output, kvpair_new(key, sum_str));
}

// Main function for testing
int main() {
    kvlist_t *input = kvlist_new();
    kvlist_append(input, kvpair_new("file1", "hello world"));
    kvlist_append(input, kvpair_new("file2", "hello again"));

    kvlist_t *output = kvlist_new();
    map_reduce(test_mapper, 2, test_reducer, 2, input, output);

    // Print results
    kvlist_iterator_t *iter = kvlist_iterator_new(output);
    kvpair_t *pair;
    printf("Output from map_reduce:\n");
    while ((pair = kvlist_iterator_next(iter)) != NULL) {
        printf("%s: %s\n", pair->key, pair->value);
    }
    kvlist_iterator_free(&iter);

    // Cleanup
    kvlist_free(&input);
    kvlist_free(&output);
    return 0;
}
