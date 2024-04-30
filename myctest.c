#include <assert.h>
#include <stdio.h>
#include <string.h>  // Include for strcmp

#include "kvlist.h"

void test_kvlist_operations() {
  kvlist_t *list = kvlist_new();
  assert(list != NULL && "List should be initialized");

  kvpair_t *pair1 = kvpair_new("key1", "value1");
  kvlist_append(list, pair1);
  assert(kvlist_get_first_key(list) != NULL &&
         "Head should not be NULL after append");
  assert(strcmp(kvlist_get_first_key(list), "key1") == 0 && "Key should match");

  kvlist_t *list2 = kvlist_new();
  kvpair_t *pair2 = kvpair_new("key2", "value2");
  kvlist_append(list2, pair2);
  kvlist_extend(list, list2);
  assert(kvlist_get_last_kv(list) == pair2 &&
         "Tail should be pair2 after extend");

  kvlist_sort(list);
  assert(strcmp(kvlist_get_first_key(list), "key1") <= 0 &&
         "List should be sorted");

  kvlist_free(&list);
  assert(list == NULL && "List should be NULL after free");

  printf("All tests passed.\n");
}

int main() {
  test_kvlist_operations();
  return 0;
}
