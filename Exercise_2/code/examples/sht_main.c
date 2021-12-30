#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf.h"
#include "ht.h"
#include "sht_file.h"
#include "hash_file.h"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
    BF_Init(LRU);

    char* filename = "/home/users/sdi1900066/YSBD2/Exercise_2/code/files/hash_files/data.db";
    int global_depth = 2;

    CALL_OR_DIE(HT_Init());

    CALL_OR_DIE(HT_CreateIndex(filename, global_depth));

    int indexDesc;
	  CALL_OR_DIE(HT_OpenIndex(filename, &indexDesc)); 

    Record record;
    srand(time(NULL));
    int r;
    printf("\nINSERT ENTRIES\n");
    for (int i = 0; i < 150; i++) {
      printf("-");
    }
    printf("\n");
    for (int id = 0; id < 10; ++id) {
      record.id = id;
      r = rand() % 12;
      memcpy(record.name, names[r], strlen(names[r]) + 1);
      r = rand() % 12;
      memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
      r = rand() % 10;
      memcpy(record.city, cities[r], strlen(cities[r]) + 1);

      printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s\n", record.id, record.name, record.surname, record.city);
      CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
    }
    printf("\n");

    printf("\n- For all entries :\n");
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
    printf("\n");

    CALL_OR_DIE(HT_CloseFile(indexDesc));
    BF_Close();

    return 0;
}
