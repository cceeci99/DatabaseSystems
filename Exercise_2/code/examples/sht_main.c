#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf.h"
#include "ht.h"
#include "sht_file.h"
#include "hash_file.h"

#define RECORDS_NUM 1000
#define GLOBAL_DEPTH 4
#define FILE_NAME "data2.db"

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

    CALL_OR_DIE(SHT_Init());

    // code here
    char *attrName = "surname";
    int attrLength = strlen(attrName);

    printf("CREATE HASH FILE\n");
    
    CALL_OR_DIE(SHT_CreateSecondaryIndex(FILE_NAME, attrName, attrLength, GLOBAL_DEPTH, "data.db"));

    printf("\nOPEN HASH FILE\n");

    int indexDesc;
    CALL_OR_DIE(SHT_OpenSecondaryIndex(FILE_NAME, &indexDesc));

    int a = 33;
    SecondaryRecord record;
    memcpy(record.index_key, "Perdikopanis", (strlen("Perdikopanis")+1));
    memcpy(&record.tupleId, &a, sizeof(int));

    CALL_OR_DIE(SHT_SecondaryInsertEntry(indexDesc, record));

    printf("CLOSE HASH FILE\n");
    CALL_OR_DIE(SHT_CloseSecondaryIndex(indexDesc));
    
    BF_Close();

    return 0;
}
