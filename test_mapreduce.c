#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvlist.h"
#include "mr.h"

// Mapper function: Splits lines into words and counts each word
void word_count_mapper(kvpair_t *kv, kvlist_t *output) {
  char *token, *saveptr;
  char *line = strdup(kv->value);  // Duplicate the string for safe tokenization
  const char *delims = " \t\n";
  token = strtok_r(line, delims, &saveptr);
  while (token) {
    kvpair_t *new_pair = kvpair_new(token, "1");
    kvlist_append(output, new_pair);
    token = strtok_r(NULL, delims, &saveptr);
  }
  free(line);
}

// Reducer function: Sums the counts for each word
void word_count_reducer(char *key, kvlist_t *list, kvlist_t *output) {
  int sum = 0;

  kvlist_iterator_t *itor = kvlist_iterator_new(list);
  kvpair_t *pair;
  while ((pair = kvlist_iterator_next(itor)) != NULL) {
    if (pair->value != NULL) {
      sum += atoi(pair->value);  // Ensure the value is not NULL before using it
    }
  }
  kvlist_iterator_free(&itor);

  char sum_string[32];
  sprintf(sum_string, "%d", sum);
  if (key != NULL) {
    kvlist_append(output, kvpair_new(key, sum_string));
  }
}

// Main function to set up and run tests
int main() {
  printf("test");
  kvlist_t *input = kvlist_new();
  kvlist_append(input,
                kvpair_new("test_input.txt",
                           "hello world hello mapreduce test mapreduce hello"));
  kvlist_append(input,
                kvpair_new("test_input.txt",
                           "goodbye hello test mapreduce world goodbye world"));

  kvlist_t *output = kvlist_new();

  // Call the map_reduce function with 2 mappers and 2 reducers
  map_reduce(word_count_mapper, 2, word_count_reducer, 2, input, output);

  // Print output to verify correct mapping
  printf("Initial Input Check\n");

  // kvlist_print(1, output);  // Print to standard output

  // Cleanup
  kvlist_free(&input);
  kvlist_free(&output);

  return 0;
}
