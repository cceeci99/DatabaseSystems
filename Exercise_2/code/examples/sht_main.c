#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf.h"
#include "hash_file.h"

// const char* names[] = {
//   "Yannis",
//   "Christofos",
//   "Sofia",
//   "Marianna",
//   "Vagelis",
//   "Maria",
//   "Iosif",
//   "Dionisis",
//   "Konstantina",
//   "Theofilos",
//   "Giorgos",
//   "Dimitris"
// };

// const char* surnames[] = {
//   "Ioannidis",
//   "Svingos",
//   "Karvounari",
//   "Rezkalla",
//   "Nikolopoulos",
//   "Berreta",
//   "Koronis",
//   "Gaitanis",
//   "Oikonomou",
//   "Mailis",
//   "Michas",
//   "Halatsis"
// };

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

char names[500][50];

char surnames[500][50];

char cities[300][50];


#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

void readRecords(){
    char* file_name = "/home/users/sdi1900066/YSBD2/Exercise_2/code/cities.txt";
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

    file_name = "/home/users/sdi1900066/YSBD2/Exercise_2/code/names.txt";
    file = fopen(file_name, "r");

    if ( file == NULL ){
        printf("Could not open file with name: '%s' \n", file_name);
        exit(EXIT_FAILURE);
    }
    
    i=0;
    while ( fgets(line, 50, file) != NULL ){
        memcpy(names[i], &line, (strlen(line)-1)*sizeof(char));  // don't include '\n'
        i++;
    }
    fclose(file);  

    file_name = "/home/users/sdi1900066/YSBD2/Exercise_2/code/surnames.txt";
    file = fopen(file_name, "r");

    if ( file == NULL ){
        printf("Could not open file with name: '%s' \n", file_name);
        exit(EXIT_FAILURE);
    }
    
    i=0;
    while ( fgets(line, 50, file) != NULL ){
        memcpy(surnames[i], &line, (strlen(line)-1)*sizeof(char));  // don't include '\n'
        i++;
    }
    fclose(file);  
}

int main() {

    BF_Init(LRU);
    
    // read_cities();
    readRecords();

    // creating first primary hash file
    char* pfilename1 = "data.db";
    char* sfilename1 = "sdata.db";

    // hash on surname
    char* index_key = "surname";

    int no_records = 1000;
    int global_depth = 2;

    CALL_OR_DIE(HT_Init());
    
    CALL_OR_DIE(HT_CreateIndex(pfilename1, global_depth));
    int pindexDesc1;
	CALL_OR_DIE(HT_OpenIndex(pfilename1, &pindexDesc1)); 


    CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename1, index_key, strlen(index_key), global_depth, pfilename1));
    int sindexDesc1;
    CALL_OR_DIE(SHT_OpenSecondaryIndex(sfilename1, &sindexDesc1));

    // set the corresponding primary' s position in open_files
    open_files[sindexDesc1].which_primary = pindexDesc1;

    Record record;
    srand(time(NULL));
    int r;
    
    UpdateRecordArray* updateArray;
    int updateArraySize;
    
    int tupleId;
    int tempid;
    
    char temp[30];

    for (int id = 0; id < no_records; ++id) {
      record.id = id;
      r = rand() % 500;
      memcpy(record.name, names[r], strlen(names[r]) + 1);
      r = rand() % 500;
      memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
      r = rand() % 300;
      memcpy(record.city, cities[r], strlen(cities[r]) + 1);

      printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s", record.id, record.name, record.surname, record.city);
      CALL_OR_DIE(HT_InsertEntry(pindexDesc1, record, &tupleId, &updateArray, &updateArraySize));
      
      if (open_files[pindexDesc1].split == 1) {

          for (int k = 0; k < MAX_OPEN_FILES; k++) {

              if (open_files[k].index_type == 0 && open_files[k].which_primary == pindexDesc1) {  // only for secondary index files with corresponding primary index file...

                  printf("Calling update for secondary index file %s on primary file %s...\n", open_files[k].filename, open_files[pindexDesc1].filename);

                  SHT_SecondaryUpdateEntry(open_files[k].fd, updateArray, updateArraySize);

              }
          }
          open_files[pindexDesc1].split = 0;
          free(updateArray);
      }

      SecondaryRecord srecord;
      memcpy(&srecord.tupleId, &tupleId, sizeof(int));      
      
      if (index_key == "city") {
          memcpy(srecord.index_key, record.city, sizeof(char)*(strlen(record.city)+1));
      }
      else if (index_key == "surname") {
          memcpy(srecord.index_key, record.surname, sizeof(char)*(strlen(record.surname)+1));
      }
      else {
          fprintf(stderr, "not available index_key for secondary index\n");
          return HT_ERROR;
      }

      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc1, srecord));
     
      printf("\n");
    }

    // // creating second primary hash file
    char* pfilename2 = "data2.db";
    char* sfilename2 = "sdata2.db";

    CALL_OR_DIE(HT_CreateIndex(pfilename2, global_depth));
    int pindexDesc2;
	CALL_OR_DIE(HT_OpenIndex(pfilename2, &pindexDesc2)); 

    CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename2, index_key, strlen(index_key), global_depth, pfilename2));
    int sindexDesc2;
    CALL_OR_DIE(SHT_OpenSecondaryIndex(sfilename2, &sindexDesc2));

    // set the corresponding primary' s position in open_files
    open_files[sindexDesc2].which_primary = pindexDesc2;

    for (int id = 0; id < no_records; ++id) {
        record.id = id;
        r = rand() % 500;
        memcpy(record.name, names[r], strlen(names[r]) + 1);
        r = rand() % 500;
        memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
        r = rand() % 300;
        memcpy(record.city, cities[r], strlen(cities[r]) + 1);

        printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s", record.id, record.name, record.surname, record.city);
        CALL_OR_DIE(HT_InsertEntry(pindexDesc2, record, &tupleId, &updateArray, &updateArraySize));
        
        if (open_files[pindexDesc2].split == 1) {

            for (int k = 0; k < MAX_OPEN_FILES; k++) {

                if (open_files[k].index_type == 0 && open_files[k].which_primary == pindexDesc2) {  // only for secondary index files with corresponding primary index file
                    SHT_SecondaryUpdateEntry(open_files[k].fd, updateArray, updateArraySize);
                }
            }
            open_files[pindexDesc2].split = 0;
            free(updateArray);
        }
        
        SecondaryRecord srecord;
        memcpy(&srecord.tupleId, &tupleId, sizeof(int));      
        
        if (index_key == "city") {
            memcpy(srecord.index_key, record.city, sizeof(char)*(strlen(record.city)+1));
            memcpy(&temp, record.city, sizeof(char)*(strlen(record.city)+1));
        }
        else if (index_key == "surname") {
            memcpy(srecord.index_key, record.surname, sizeof(char)*(strlen(record.surname)+1));
            memcpy(&temp, record.surname, sizeof(char)*(strlen(record.surname)+1));
        }
        else {
            fprintf(stderr, "not available index_key for secondary index\n");
            return HT_ERROR;
        }

        CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc2, srecord));

        printf("\n");
    }
    
    printf("\n");
    
    CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc1, temp));
    printf("\n");
    
    CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc2, temp));
    printf("\n");
    
    CALL_OR_DIE(SHT_InnerJoin(sindexDesc1, sindexDesc2, temp));
    printf("\n");

    CALL_OR_DIE(HT_CloseFile(pindexDesc1));
    CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc1));

    CALL_OR_DIE(HT_CloseFile(pindexDesc2));
    CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc2));

    BF_Close();

    return 0;
}