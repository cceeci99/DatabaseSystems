#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>

#include "bf.h"
#include "hash_file.h"
#include "data.h"

// can be changed 
#define NO_RECORDS 100
#define GLOBAL_DEPTH 3
#define INDEX_KEY "surname"

// init sizes of names, surnames is 500 and cities is 300, they can be changed to see better results for InnerJoin
#define NO_NAMES 20
#define NO_SURNAMES 20
#define NO_CITIES 15


#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }
  

int main() {
    char no_rec[10];
    sprintf(no_rec, "%d", NO_RECORDS);

    char depth[10];
    sprintf(depth, "%d", GLOBAL_DEPTH);

    char* text = "files/logs/result_";
	char* results = malloc((strlen(no_rec) + strlen(depth) + strlen(text) + 6) * sizeof(char));
	results[0] = '\0';
	strcat(results, text);
	strcat(results, no_rec);
	text = "_";
	strcat(results, text);
	strcat(results, depth);
	text = ".txt";
	strcat(results, text);

    int fd = open(results, O_CREAT | O_WRONLY, 0644);
	if (fd == -1) {
		perror("open() failed");
		exit(EXIT_FAILURE);
	}

	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	dup2(fd, STDOUT_FILENO);
	dup2(fd, STDERR_FILENO);

    BF_Init(LRU);

    // creating first primary hash file
    char* pfilename1 = "data.db";
    char* sfilename1 = "sdata.db";

    CALL_OR_DIE(HT_Init());
    
    printf("CREATE FIRST PRIMARY HASH FILE\n");
    for (int i = 0; i < 150; i++){
        printf("-");
    }
    printf("\n");
    
    CALL_OR_DIE(HT_CreateIndex(pfilename1, GLOBAL_DEPTH));
    printf("\n");
    
    printf("\nOPEN FIRST PRIMARY HASH FILE\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
    printf("\n");
    
    int pindexDesc1;
	CALL_OR_DIE(HT_OpenIndex(pfilename1, &pindexDesc1)); 
    printf("\n");
    
    printf("CREATE SECONDARY HASH FILE\n");
    for (int i = 0; i < 150; i++){
        printf("-");
    }
    printf("\n");
    
    CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename1, INDEX_KEY, strlen(INDEX_KEY)+1, GLOBAL_DEPTH, pfilename1));
    printf("\n");

    printf("\nOPEN SECONDARY HASH FILE\n");
	for (int i = 0; i < 150; i++) { 
		printf("-");
	}
    printf("\n");

    int sindexDesc1;
    CALL_OR_DIE(SHT_OpenSecondaryIndex(sfilename1, &sindexDesc1));
    printf("\n");

    // set the corresponding primary' s position in open_files
    open_files[sindexDesc1].which_primary = pindexDesc1;

    Record record;
    srand(time(NULL));
    int r;
    
    printf("\nINSERT ENTRIES\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");

    int tupleId;    
    UpdateRecordArray* updateArray;
    int updateArraySize;
    
    for (int id = 0; id < NO_RECORDS; ++id) {
        record.id = id;

        r = rand() % NO_NAMES;
        memcpy(record.name, names[r], strlen(names[r]) + 1);
        
        r = rand() % NO_SURNAMES;
        memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
        
        r = rand() % NO_CITIES;
        memcpy(record.city, cities[r], strlen(cities[r]) + 1);

        // printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s", record.id, record.name, record.surname, record.city);
        CALL_OR_DIE(HT_InsertEntry(pindexDesc1, record, &tupleId, &updateArray, &updateArraySize));
        
        if (open_files[pindexDesc1].split == 1) {

            for (int k = 0; k < MAX_OPEN_FILES; k++) {

                if (open_files[k].index_type == 0 && open_files[k].which_primary == pindexDesc1) {  // only for secondary index files with corresponding primary index file...
                
                    SHT_SecondaryUpdateEntry(open_files[k].fd, updateArray, updateArraySize);
                }
            }
            open_files[pindexDesc1].split = 0;
            free(updateArray);
        }

        SecondaryRecord srecord;
        memcpy(&srecord.tupleId, &tupleId, sizeof(int));      
        
        if (INDEX_KEY == "city") {
            memcpy(srecord.index_key, record.city, sizeof(char)*(strlen(record.city)+1));
        }
        else if (INDEX_KEY == "surname") {
            memcpy(srecord.index_key, record.surname, sizeof(char)*(strlen(record.surname)+1));
        }
        else {
            fprintf(stderr, "not available index_key for secondary index\n");
            return HT_ERROR;
        }

        CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc1, srecord));
    }

    printf("\n");

	// Print entries
	printf("\nPRINT ENTRIES\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");

	int id = rand() % NO_RECORDS;
	printf("- For id %d :\n", id);
	CALL_OR_DIE(HT_PrintAllEntries(pindexDesc1, &id));	
	printf("\n- For all entries :\n");
	CALL_OR_DIE(HT_PrintAllEntries(pindexDesc1, NULL));
	printf("\n");

    printf("\nPRINT ENTRIES\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");

    // print all records from 1st secondary index hash file
    printf("\n- For all entries :\n");
    CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc1, NULL));
    printf("\n");

    // print random record from 1st secondary index hash file
    char temp[30];
    if (strcmp(INDEX_KEY, "city") == 0) {
        r = rand() % NO_CITIES;
        memcpy(&temp, cities[r], sizeof(char)*(strlen(cities[r])+1)); 
    }
    else if (strcmp(INDEX_KEY, "surname") == 0){
        r = rand() % NO_SURNAMES;
        memcpy(&temp, surnames[r], sizeof(char)*(strlen(surnames[r])+1)); 
    }
    else {
        fprintf(stderr, "not available index_key\n");
        return HT_ERROR;
    }
    printf("- For %s = %s\n", INDEX_KEY, temp);
    CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc1, temp));
    printf("\n");
    
    // creating second primary hash file
    char* pfilename2 = "data2.db";
    char* sfilename2 = "sdata2.db";

    CALL_OR_DIE(HT_CreateIndex(pfilename2, GLOBAL_DEPTH));
    int pindexDesc2;
	CALL_OR_DIE(HT_OpenIndex(pfilename2, &pindexDesc2)); 

    CALL_OR_DIE(SHT_CreateSecondaryIndex(sfilename2, INDEX_KEY, strlen(INDEX_KEY)+1, GLOBAL_DEPTH, pfilename2));
    int sindexDesc2;
    CALL_OR_DIE(SHT_OpenSecondaryIndex(sfilename2, &sindexDesc2));

    // set the corresponding primary' s position in open_files
    open_files[sindexDesc2].which_primary = pindexDesc2;

    for (int id = 0; id < NO_RECORDS; ++id) {
        record.id = id;

        r = rand() % NO_NAMES;
        memcpy(record.name, names[r], strlen(names[r]) + 1);
        
        r = rand() % NO_SURNAMES;
        memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
        
        r = rand() % NO_CITIES;
        memcpy(record.city, cities[r], strlen(cities[r]) + 1);

        // printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s", record.id, record.name, record.surname, record.city);
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
        
        if (INDEX_KEY == "city") {
            memcpy(srecord.index_key, record.city, sizeof(char)*(strlen(record.city)+1));
        }
        else if (INDEX_KEY == "surname") {
            memcpy(srecord.index_key, record.surname, sizeof(char)*(strlen(record.surname)+1));
        }
        else {
            fprintf(stderr, "not available index_key for secondary index\n");
            return HT_ERROR;
        }

        CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc2, srecord));
    }
    
    printf("\n");

    // print secondary indexes of 2 hash files 
    // Test InnerJoin on random index_key = temp
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
    
    printf("\n");

    CALL_OR_DIE(SHT_HashStatistics(sfilename1));
    CALL_OR_DIE(SHT_HashStatistics(sfilename2));

    BF_Close();

    return 0;
}