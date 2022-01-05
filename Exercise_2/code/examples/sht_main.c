#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf.h"
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

    char* pfilename = "data.db";
    char* sfilename = "data2.db";
    int global_depth = 2;

    CALL_OR_DIE(HT_Init());
    CALL_OR_DIE(HT_CreateIndex(pfilename, global_depth));

    CALL_OR_DIE(SHT_Init());
    CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename, "city", strlen("city")+1, global_depth, "data.db"));
    // CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename, "surname", strlen("surname")+1, global_depth, "data.db"));

    int indexDesc;
    int sindexDesc;
	  CALL_OR_DIE(HT_OpenIndex(pfilename, &indexDesc)); 
    CALL_OR_DIE(SHT_OpenSecondaryIndex(sfilename, &sindexDesc));

    Record record;
    srand(time(NULL));
    int r;
    
    printf("\nINSERT ENTRIES\n");
    for (int i = 0; i < 150; i++) {
      printf("-");
    }
    printf("\n");

    UpdateRecordArray* updateArray;

    char temp[30];
    for (int id = 0; id < 140; ++id) {
      record.id = id;
      r = rand() % 12;
      memcpy(record.name, names[r], strlen(names[r]) + 1);
      r = rand() % 12;
      memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
      r = rand() % 10;
      memcpy(record.city, cities[r], strlen(cities[r]) + 1);

      int updateArraySize;
      int tupleId;

      printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s", record.id, record.name, record.surname, record.city);
      CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, &updateArray, &updateArraySize));
      
      if ( open_files[indexDesc].split == 1) {

          for (int k = 0; k < MAX_OPEN_FILES; k++) {

              if (open_files[k].index_type == 0) {  // only for secondary index files with corresponding primary index file...

                  // update()...

                  printf("Calling update for secondary index file %s\n", open_files[k].filename);
                  SHT_SecondaryUpdateEntry(open_files[k].fd, updateArray, updateArraySize);

              }
          }
          open_files[indexDesc].split = 0;
          free(updateArray);
      }

      SecondaryRecord srecord;
      memcpy(srecord.index_key, record.city, strlen(record.city)+1);
      // memcpy(srecord.index_key, record.surname, strlen(record.surname)+1);
      memcpy(&srecord.tupleId, &tupleId, sizeof(int));

      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc, srecord));

      printf("\n");
    }
    
    printf("\n");
    
    CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc, NULL));

    CALL_OR_DIE(HT_CloseFile(indexDesc));
    CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc));

    BF_Close();

    return 0;
}