#include "mr.h"

#include "hash.h"
#include "kvlist.h"
#define _GNU_SOURCE

#include <pthread.h>
#include <search.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct hsearch_data ht_tab;  // Global hash table

typedef struct {
  int count;
} count_data;

struct hsearch_data ht_tab;  // Hash table
char **keys;                 // Array to store keys
int num_keys = 0;            // Number of keys stored

void init_hash_table() {
  if (!hcreate_r(10000, &ht_tab)) {
    perror("Failed to create hash table");
    exit(EXIT_FAILURE);
  }
  keys =
      malloc(10000 *
             sizeof(char *));  // Allocate space for keys, adjust size as needed
  if (!keys) {
    perror("Failed to allocate memory for keys");
    exit(EXIT_FAILURE);
  }
}

void add_to_hash_table(char *key) {
  ENTRY item = {.key = strdup(key), .data = NULL};
  ENTRY *found_item;

  if (!hsearch_r(item, FIND, &found_item, &ht_tab)) {
    count_data *data = malloc(sizeof(count_data));
    if (!data) {
      perror("Failed to allocate memory for count data");
      exit(EXIT_FAILURE);
    }
    data->count = 1;
    item.data = data;
    if (!hsearch_r(item, ENTER, &found_item, &ht_tab)) {
      fprintf(stderr, "Hash table insertion error\n");
      free(item.key);
      free(data);
      return;
    }
    keys[num_keys++] = strdup(key);  // Store the key in the array
  } else {
    free(item.key);  // Free the duplicated key if already present
    ((count_data *)found_item->data)->count++;
  }
}

void print_hash_table() {
  for (int i = 0; i < num_keys; i++) {
    ENTRY item = {.key = keys[i], .data = NULL};
    ENTRY *found_item;
    if (hsearch_r(item, FIND, &found_item, &ht_tab)) {
      printf("%s: %d\n", found_item->key,
             ((count_data *)found_item->data)->count);
    }
  }
}

void cleanup() {
  hdestroy_r(&ht_tab);
  for (int i = 0; i < num_keys; i++) {
    free(keys[i]);  // Free each stored key
  }
  free(keys);  // Free the array of keys
}

typedef struct {
  mapper_t mapper;
  kvlist_t *input;
  kvlist_t *output;
  size_t start;
  size_t end;
} MapperArgs;

pthread_mutex_t output_mutex;

// Function to be executed by mapper threads
void *mapper_function(void *args) {
  MapperArgs *mapperArgs = (MapperArgs *)args;
  // Debug: Print what is being processed

  kvlist_iterator_t *iter = kvlist_iterator_new(mapperArgs->input);
  kvpair_t *pair;
  size_t index = 0;

  // Iterate over assigned segment
  while ((pair = kvlist_iterator_next(iter)) != NULL &&
         index < mapperArgs->end) {
    if (index >= mapperArgs->start) {
      // printf("Thread processing key: %s, value: %s\n", pair->key,
      // pair->value);

      mapperArgs->mapper(pair, mapperArgs->output);
    }
    index++;
  }
  kvlist_iterator_free(&iter);

  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t *input, kvlist_t *output) {
  pthread_t *threads = malloc(num_mapper * sizeof(pthread_t));
  MapperArgs *mapperArgs = malloc(num_mapper * sizeof(MapperArgs));
  pthread_mutex_init(&output_mutex, NULL);  // Initialize mutex
  if (reducer == NULL && num_reducer == 0) {
    // Log or simply run an empty statement
  }
  // Split the input data among the mapper threads
  size_t total_items = kvlist_count(input);
  size_t items_per_thread = total_items / num_mapper;
  size_t extra_items = total_items % num_mapper;
  size_t start = 0;

  for (size_t i = 0; i < num_mapper; i++) {
    size_t end = start + items_per_thread + (i < extra_items ? 1 : 0);
    mapperArgs[i].mapper = mapper;
    mapperArgs[i].input = input;
    mapperArgs[i].output =
        kvlist_new();  // Individual output list for each thread
    mapperArgs[i].start = start;
    mapperArgs[i].end = end;
    start = end;

    pthread_create(&threads[i], NULL, mapper_function, &mapperArgs[i]);
  }

  // Wait for all mapper threads to complete
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(threads[i], NULL);
    // Merge thread outputs after all threads are joined
    kvlist_extend(output, mapperArgs[i].output);
    kvlist_free(&mapperArgs[i].output);
  }

  // Cleanup
  free(threads);
  free(mapperArgs);
  pthread_mutex_destroy(&output_mutex);  // Destroy mutex
}

// This function assumes count_data struct holds an integer count.
void reducerz(char *key, count_data *data, kvlist_t *output) {
  char count_string[20];
  sprintf(count_string, "%d", data->count);  // Convert integer count to string
  kvpair_t *new_pair = kvpair_new(key, count_string);
  kvlist_append(output, new_pair);  // Append to output list
}

void execute_reduce_phase(kvlist_t *output) {
  // Iterate over all keys stored in the keys array
  for (int i = 0; i < num_keys; i++) {
    ENTRY item = {.key = keys[i], .data = NULL};
    ENTRY *found_item;
    if (hsearch_r(item, FIND, &found_item, &ht_tab)) {
      reducerz(found_item->key, (count_data *)found_item->data, output);
    }
  }
}

// Utility function to count items in a kvlist
size_t kvlist_count(kvlist_t *list) {
  size_t count = 0;
  kvlist_iterator_t *iter = kvlist_iterator_new(list);
  while (kvlist_iterator_next(iter) != NULL) {
    count++;
  }
  kvlist_iterator_free(&iter);
  return count;
}
