#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvlist.h"
#include "mr.h"

void file_read(kvlist_t *input, const char *filename);
void init_hash_table(void);
void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t *input, kvlist_t *output);
void reduc(kvlist_t *output);
void print_kvlist(kvlist_t *list);
void cleanup(void);

void simple_mapper(kvpair_t *kv, kvlist_t *output) {
  char *line = strdup(kv->value);
  if (!line) return;

  char *token, *saveptr = NULL;

  for (token = strtok_r(line, " ", &saveptr); token != NULL;
       token = strtok_r(NULL, " ", &saveptr)) {
    add_to_hash_table(token);
  }

  free(line);
}

void print_kvlist(kvlist_t *list) {
  kvlist_iterator_t *iterator = kvlist_iterator_new(list);
  kvpair_t *current_pair;

  // printf("output:\n");

  for (current_pair = kvlist_iterator_next(iterator); current_pair != NULL;
       current_pair = kvlist_iterator_next(iterator)) {
    printf("%s,%s\n", current_pair->key, current_pair->value);
  }

  kvlist_iterator_free(&iterator);
}

// Function to create a predefined input list for testing
// kvlist_t *create_test_input() {
//   kvlist_t *input = kvlist_new();
//   kvlist_append(
//       input,
//       kvpair_new(
//           "file1",
//           "Hello world, hello again! Is this the real world, or is it just "
//           "fantasy Caught in a landslide, no escape from reality."));
//   kvlist_append(input, kvpair_new("file2", "hello mapreduce mapreduce"));
//   return input;
// }

void file_read(kvlist_t *input, const char *filename) {
  FILE *file = fopen(filename, "r");
  if (!file) {
    perror("failed to open file");
    exit(EXIT_FAILURE);
  }

  char *current_line = NULL;
  size_t line_capacity = 0;
  ssize_t line_size;

  while ((line_size = getline(&current_line, &line_capacity, file)) != -1) {
    if (current_line[line_size - 1] == '\n') {
      current_line[line_size - 1] = '\0';
    }
    kvlist_append(input, kvpair_new(filename, strdup(current_line)));
  }

  free(current_line);
  fclose(file);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s <file1> [file2] ... [fileN]\n", argv[0]);
    return EXIT_FAILURE;
  }

  kvlist_t *input = kvlist_new();
  kvlist_t *output = kvlist_new();

  for (int i = 1; i < argc; i++) {
    file_read(input, argv[i]);
  }

  init_hash_table();

  map_reduce(simple_mapper, argc - 1, NULL, 0, input, output);
  reduc(output);

  print_kvlist(output);

  free_keys();
  kvlist_free(&input);
  kvlist_free(&output);

  return 0;
}
