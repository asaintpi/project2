#include "mr.h"

#include "hash.h"
#include "kvlist.h"
#define _GNU_SOURCE

#include <pthread.h>
#include <search.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


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

void init_hash_table() {
  if (!hcreate_r(10000, &htable)) {
    exit(EXIT_FAILURE);
  }
  keys =
      malloc(10000 *
             sizeof(char *)); 
  if (!keys) {
    exit(EXIT_FAILURE);
  }
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

    for (pair = kvlist_iterator_next(iter); pair != NULL && index < maps->end; pair = kvlist_iterator_next(iter), index++) {
        if (index >= maps->start) {

            maps->mapper(pair, maps->output);
        }
    }
    kvlist_iterator_free(&iter);

    return NULL;
}


void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t *input, kvlist_t *output) {
    pthread_t *threads = (pthread_t *)malloc(num_mapper * sizeof(pthread_t));
    Mapper *maps = (Mapper *)malloc(num_mapper * sizeof(Mapper));

    pthread_mutex_init(&omute, NULL);

    if (reducer == NULL && num_reducer == 0) {
        // not using reducer or num reducer so
    }

    size_t count = kvlist_count(input), per_thread = count / num_mapper, remainder = count % num_mapper;
    size_t start = 0, end;

    for (size_t i = 0; i < num_mapper; i++) {
        end = start + per_thread + (i < remainder ? 1 : 0);
        maps[i] = (Mapper){ .mapper = mapper, .input = input, .output = kvlist_new(), .start = start, .end = end };
        pthread_create(&threads[i], NULL, mapper_function, &maps[i]);
        start = end;
    }

    for (size_t i = 0; i < num_mapper; i++) {
        pthread_join(threads[i], NULL);
        kvlist_extend(output, maps[i].output);
        kvlist_free(&maps[i].output);
    }

    free(threads);
    free(maps);
    pthread_mutex_destroy(&omute);
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

    for (; kvlist_iterator_next(iterator); count++);

    kvlist_iterator_free(&iterator);

    return count;  
}
