#pragma once

#include <stddef.h>

#include "kvlist.h"

// Declare the structure before using it in function prototypes.
typedef struct {
  char* key;
  kvlist_t* values;
} ShuffledData;

typedef void (*mapper_t)(kvpair_t* kv, kvlist_t* output);
typedef void (*reducer_t)(char* key, kvlist_t* list, kvlist_t* output);
void add_to_hash_table(char* key);
void print_hash_table();
void init_hash_table();
void free_keys();
void reduc(kvlist_t* output);
void add_to_shuffled_data(ShuffledData* shuffled, size_t num_keys,
                          kvpair_t* pair);
void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output);
size_t kvlist_count(kvlist_t* list);
