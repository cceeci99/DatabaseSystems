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

// const char* cities[] = {
//   "Athens",
//   "San Francisco",
//   "Los Angeles",
//   "Amsterdam",
//   "London",
//   "New York",
//   "Tokyo",
//   "Hong Kong",
//   "Munich",
//   "Miami"
// };

char cities[300][50];

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

void read_cities(){
    char* file_name = "/home/users/sdi1900066/YSBD2/Exercise_2/code/examples/cities.txt";
    FILE *file = fopen(file_name, "r");

    if ( file == NULL ){
        printf("Could not open file with name: '%s' \n", file_name);
        exit(EXIT_FAILURE);
    }
    
    char line[50];
    int i=0;
    while ( fgets(line, 50, file) != NULL ){
        memcpy(cities[i], &line, (strlen(line)-1)*sizeof(char));  // don't include '\n'
        i++;
    }
    fclose(file);
}

int main() {
    BF_Init(LRU);
    
    read_cities();

    char* pfilename = "data.db";
    char* sfilename = "data2.db";
    // char* ssfilename = "data3.db";

    int global_depth = 2;

    CALL_OR_DIE(HT_Init());
    CALL_OR_DIE(HT_CreateIndex(pfilename, global_depth));

    CALL_OR_DIE(SHT_Init());
    CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename, "city", strlen("city"), global_depth, "data.db"));
    // CALL_OR_DIE(SHT_CreateSecondaryIndex(ssfilename, "surname", strlen("surname"), global_depth, "data.db"));

    int indexDesc;
    int sindexDesc;
    // int ssindexDesc;

	  CALL_OR_DIE(HT_OpenIndex(pfilename, &indexDesc)); 
    CALL_OR_DIE(SHT_OpenSecondaryIndex(sfilename, &sindexDesc));
    // CALL_OR_DIE(SHT_OpenSecondaryIndex(ssfilename, &ssindexDesc));
    

    Record record;
    srand(time(NULL));
    int r;
    
    printf("\nINSERT ENTRIES\n");
    for (int i = 0; i < 150; i++) {
      printf("-");
    }
    printf("\n");

    UpdateRecordArray* updateArray;
    int updateArraySize;
    
    int tupleId;

    char temp[30];
    for (int id = 0; id < 100; ++id) {
      record.id = id;
      r = rand() % 12;
      memcpy(record.name, names[r], strlen(names[r]) + 1);
      r = rand() % 12;
      memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
      r = rand() % 300;
      memcpy(record.city, cities[r], strlen(cities[r]) + 1);


      printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s", record.id, record.name, record.surname, record.city);
      CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, &updateArray, &updateArraySize));
      
      if ( open_files[indexDesc].split == 1) {

          for (int k = 0; k < MAX_OPEN_FILES; k++) {

              if (open_files[k].index_type == 0) {  // only for secondary index files with corresponding primary index file...

                  printf("Calling update for secondary index file %s ...\n", open_files[k].filename);

                  SHT_SecondaryUpdateEntry(open_files[k].fd, updateArray, updateArraySize);

              }
          }
          open_files[indexDesc].split = 0;
          free(updateArray);
      }

      SecondaryRecord srecord;
      memcpy(srecord.index_key, record.city, strlen(record.city)+1);
      memcpy(&srecord.tupleId, &tupleId, sizeof(int));

      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc, srecord));

      // SecondaryRecord ssrecord;
      // memcpy(ssrecord.index_key, record.surname, strlen(record.surname)+1);
      // memcpy(&ssrecord.tupleId, &tupleId, sizeof(int));

      // CALL_OR_DIE(SHT_SecondaryInsertEntry(ssindexDesc, ssrecord));

      printf("\n");
    }
    
    printf("\n");
    
    CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc, NULL));
    // CALL_OR_DIE(SHT_PrintAllEntries(ssindexDesc, NULL));
    

    CALL_OR_DIE(HT_CloseFile(indexDesc));
    CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc));
    // CALL_OR_DIE(SHT_CloseSecondaryIndex(ssindexDesc));


    BF_Close();

    return 0;
}