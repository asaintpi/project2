#include "mr.h"

#include "hash.h"
#include "kvlist.h"
#define _GNU_SOURCE

#include <pthread.h>
#include <search.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h> 

typedef struct {
  mapper_t mapper;
  kvlist_t *input;
  kvlist_t *output;
  size_t start;
  size_t end;
} Mapper;

struct hsearch_data htable;

typedef struct {
  int c;
} dt;

struct hsearch_data htable;
char **keys;
int nkey = 0;

#define TABLE_SIZE 10000  

typedef struct HashNode {
    char *key;
    int value;
    struct HashNode *next;
} HashNode;

typedef struct HashTable {
    HashNode **buckets;
} HashTable;

HashTable* createHashTable() {
    HashTable *table = malloc(sizeof(HashTable));
    if (!table) return NULL;

    table->buckets = malloc(sizeof(HashNode*) * TABLE_SIZE);
    if (!table->buckets) {
        free(table);
        return NULL;
    }

    memset(table->buckets, 0, sizeof(HashNode*) * TABLE_SIZE);
    return table;
}

void freeHashTable(HashTable *table) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode *node = table->buckets[i];
        while (node) {
            HashNode *temp = node;
            node = node->next;
            free(temp->key);
            free(temp);
        }
    }
    free(table->buckets);
    free(table);
}

unsigned int hasht(const char *key) {
    unsigned long int value = 0;
    for (; *key != '\0'; key++) value = value * 37 + *key;
    return value % TABLE_SIZE;
}

HashNode* createHashNode(const char *key, int value) {
    HashNode *node = malloc(sizeof(HashNode));
    if (!node) return NULL;
    node->key = strdup(key);
    node->value = value;
    node->next = NULL;
    return node;
}

int addOrUpdate(HashTable *table, const char *key, int value) {
    unsigned int bucketIndex = hasht(key);
    HashNode *node = table->buckets[bucketIndex];
    while (node) {
        if (strcmp(node->key, key) == 0) {
            node->value += value;  // Update existing value
            return 0;
        }
        node = node->next;
    }
    // Insert new node
    HashNode *newNode = createHashNode(key, value);
    if (!newNode) return -1;
    newNode->next = table->buckets[bucketIndex];
    table->buckets[bucketIndex] = newNode;
    return 0;
}


void init_hash_table() {
  if (!hcreate_r(1000, &htable)) {
    exit(EXIT_FAILURE);
  }
  keys = malloc(10000 * sizeof(char *));
  if (!keys) {
    exit(EXIT_FAILURE);
  }
}

kvlist_t** shuffle(Mapper *mappers, size_t num_mappers, size_t num_reducers) {
    kvlist_t** reducer_inputs = malloc(num_reducers * sizeof(kvlist_t*));
    for (size_t i = 0; i < num_reducers; i++) {
        reducer_inputs[i] = kvlist_new();  // Initialize lists for each reducer
    }

    for (size_t i = 0; i < num_mappers; i++) {
        kvlist_iterator_t* it = kvlist_iterator_new(mappers[i].output);
        kvpair_t* pair;
        while ((pair = kvlist_iterator_next(it)) != NULL) {
            unsigned long hash_value = hash(pair->key) % num_reducers;
            // Properly group data for reducers by key
            kvlist_append(reducer_inputs[hash_value], kvpair_new(pair->key, pair->value));
                        //printf("Shuffled key: %s to reducer: %lu\n", pair->key, hash_value);

        }
        kvlist_iterator_free(&it);
    }

    return reducer_inputs;
}

void add_to_hash_table(char *key) {
  ENTRY item;
  item.key = strdup(key);
  item.data = NULL;
  ENTRY *found_item;

  int result = hsearch_r(item, FIND, &found_item, &htable);
  if (!result) {
    dt *data = (dt *)malloc(sizeof(dt));
    if (data == NULL) {
      perror("adding htable function fail");
      exit(EXIT_FAILURE);
    }
    data->c = 1;
    item.data = data;

    if (!hsearch_r(item, ENTER, &found_item, &htable)) {
      free(item.key);
      free(data);
    } else {
      keys[nkey++] = item.key;
    }
  } else {
    ((dt *)found_item->data)->c++;
    free(item.key);  // free dup keys
  }
}

void print_hash_table() {
  int i = 0;
  while (i < nkey) {
    ENTRY item;
    item.key = keys[i];
    item.data = NULL;
    ENTRY *found_item;

    if (hsearch_r(item, FIND, &found_item, &htable)) {
      dt *data = (dt *)found_item->data;
      printf("%s: %d\n", found_item->key, data->c);
    }
    i++;
  }
}

void free_keys() {
  hdestroy_r(&htable);
  int idx = 0;
  while (idx < nkey) {
    free(keys[idx]);
    idx++;
  }
  free(keys);
}

pthread_mutex_t omute;

void *mapper_function(void *args) {
  Mapper *maps = (Mapper *)args;

  kvlist_iterator_t *iter = kvlist_iterator_new(maps->input);
  kvpair_t *pair;
  size_t index = 0;

  for (pair = kvlist_iterator_next(iter); pair != NULL && index < maps->end;
       pair = kvlist_iterator_next(iter), index++) {
    if (index >= maps->start) {
      maps->mapper(pair, maps->output);
            //printf("Mapped key: %s, value: %s\n", pair->key, pair->value);

    }
  }
  kvlist_iterator_free(&iter);

  return NULL;
}

void collectResults(HashTable *ht, kvlist_t *output) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode *node = ht->buckets[i];
        while (node) {
            char str[20];
            sprintf(str, "%d", node->value);
            kvlist_append(output, kvpair_new(strdup(node->key), strdup(str)));
            node = node->next;
        }
    }
}

void* reducer_function(void* arg) {
    kvlist_t* input_list = (kvlist_t*)arg;
    kvlist_t* output = kvlist_new();
    HashTable *ht = createHashTable();

    if (!ht) {
        fprintf(stderr, "Failed to create hash table\n");
        return output;
    }

    kvlist_iterator_t* itor = kvlist_iterator_new(input_list);
    kvpair_t* pair;

    while ((pair = kvlist_iterator_next(itor)) != NULL) {
        int value = atoi(pair->value);
        if (addOrUpdate(ht, pair->key, value) != 0) {
            fprintf(stderr, "Failed to update hash table\n");
        }
    }

    // Assuming you'll implement a function to collect results from the hash table
    collectResults(ht, output);

    kvlist_iterator_free(&itor);
    freeHashTable(ht);
    return output;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t *input, kvlist_t *output) {
    pthread_t *mapper_threads = malloc(num_mapper * sizeof(pthread_t));
    Mapper *maps = malloc(num_mapper * sizeof(Mapper));
    size_t count = kvlist_count(input);
    size_t per_thread = count / num_mapper;
    size_t remainder = count % num_mapper;
    size_t start = 0, end;

    // Spawn mapper threads
    for (size_t i = 0; i < num_mapper; i++) {
        end = start + per_thread + (i < remainder ? 1 : 0);
        maps[i] = (Mapper){ .mapper = mapper, .input = input, .output = kvlist_new(), .start = start, .end = end };
        pthread_create(&mapper_threads[i], NULL, mapper_function, &maps[i]);
        start = end;
    }

    // Wait for all mapper threads to finish
    for (size_t i = 0; i < num_mapper; i++) {
        pthread_join(mapper_threads[i], NULL);
    }

    // Shuffle phase
    kvlist_t** reducer_inputs = shuffle(maps, num_mapper, num_reducer);

    // Free mapper resources and prepare for reducing
    for (size_t i = 0; i < num_mapper; i++) {
        kvlist_free(&maps[i].output);
    }
    free(maps);
    free(mapper_threads);

    // Prepare reducer threads
    pthread_t *reducer_threads = malloc(num_reducer * sizeof(pthread_t));
   // printf("Reducer threads launched\n");

    // Spawn reducer threads
    for (size_t i = 0; i < num_reducer; i++) {
        pthread_create(&reducer_threads[i], NULL, reducer_function, reducer_inputs[i]);
    }

    // Wait for all reducer threads to finish and collect output
    for (size_t i = 0; i < num_reducer; i++) {
        void *reducer_output;
        pthread_join(reducer_threads[i], &reducer_output);
        kvlist_extend(output, (kvlist_t *)reducer_output);
        kvlist_free((kvlist_t **)&reducer_output);
    }

    // Free reducer resources
    for (size_t i = 0; i < num_reducer; i++) {
        kvlist_free(&reducer_inputs[i]);
    }
    free(reducer_inputs);
    free(reducer_threads);
}

void reducerz(char *key, dt *data, kvlist_t *output) {
  char str_cnt[20];
  snprintf(str_cnt, sizeof(str_cnt), "%d", data->c);

  kvpair_t *new_kv_pair = kvpair_new(key, str_cnt);

  kvlist_append(output, new_kv_pair);
}

void reduc(kvlist_t *output) {
  int idx = 0;
  ENTRY item, *found_item;

  while (idx < nkey) {
    item = (ENTRY){.key = keys[idx], .data = NULL};

    if (hsearch_r(item, FIND, &found_item, &htable)) {
      reducerz(found_item->key, (dt *)found_item->data, output);
    }
    idx++;
  }
}

size_t kvlist_count(kvlist_t *list) {
  size_t count = 0;
  kvlist_iterator_t *iterator = kvlist_iterator_new(list);

  for (; kvlist_iterator_next(iterator); count++)
    ;

  kvlist_iterator_free(&iterator);

  return count;
}
