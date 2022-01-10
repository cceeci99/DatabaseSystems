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
#define NO_RECORDS 50
#define GLOBAL_DEPTH 2
#define INDEX_KEY "surname"

// init sizes of names, surnames is 500 and cities is 300, they can be changed to see better results for InnerJoin
#define NO_NAMES 500
#define NO_SURNAMES 500
#define NO_CITIES 300


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

    char* temp = "files/logs/result_";
	char* results = malloc((strlen(no_rec) + strlen(temp) + 6) * sizeof(char));
	results[0] = '\0';
	strcat(results, temp);
	strcat(results, no_rec);
	temp = ".txt";
	strcat(results, temp);

    int fd = open(results, O_CREAT | O_WRONLY, 0644);
	if (fd == -1) {
		perror("open() failed");
		exit(EXIT_FAILURE);
	}

	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	dup2(fd, STDOUT_FILENO);
	dup2(fd, STDERR_FILENO);

    srand(time(NULL));

    BF_Init(LRU);

    CALL_OR_DIE(HT_Init());

    char pfiles[2][40];
    char pindexes[2];

    char sfiles[2][40];
    char sindexes[2];

    for (int f = 0; f < 2; f++) {
        char fn[5];
        sprintf(fn, "%d", f+1);

        strcpy(pfiles[f], "files/hash_files/primary_data_");
        strcat(pfiles[f], fn);
        strcat(pfiles[f], ".db");

        strcpy(sfiles[f], "files/hash_files/secondary_data_");
        strcat(sfiles[f], fn);
        strcat(sfiles[f], ".db");

        printf("CREATE PRIMARY INDEX HASH FILE\n");
        for (int i = 0; i < 150; i++){
            printf("-");
        }
        printf("\n");

        CALL_OR_DIE(HT_CreateIndex(pfiles[f], GLOBAL_DEPTH));
        printf("\n");

        printf("\nOPEN PRIMARY INDEX HASH FILE\n");
        for (int i = 0; i < 150; i++) {
            printf("-");
        }
        printf("\n");

        int pindexDesc;
	    CALL_OR_DIE(HT_OpenIndex(pfiles[f], &pindexDesc)); 
        pindexes[f] = pindexDesc;
        printf("\n");

        printf("CREATE SECONDARY INDEX HASH FILE\n");
        for (int i = 0; i < 150; i++){
            printf("-");
        }
        printf("\n");

        CALL_OR_DIE(SHT_CreateSecondaryIndex(sfiles[f], INDEX_KEY, strlen(INDEX_KEY)+1, GLOBAL_DEPTH, pfiles[f]));
        printf("\n");

        printf("\nOPEN SECONDARY INDEX HASH FILE\n");
        for (int i = 0; i < 150; i++) { 
            printf("-");
        }
        printf("\n");

        int sindexDesc;
        CALL_OR_DIE(SHT_OpenSecondaryIndex(sfiles[f], &sindexDesc));
        sindexes[f] = sindexDesc;
        printf("\n");

        // set the corresponding primary' s position in open_files
        open_files[sindexDesc].which_primary = pindexDesc;

        Record record;  
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

            printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s\n", record.id, record.name, record.surname, record.city);
            CALL_OR_DIE(HT_InsertEntry(pindexDesc, record, &tupleId, &updateArray, &updateArraySize));
            
            if (open_files[pindexDesc].split == 1) {

                for (int k = 0; k < MAX_OPEN_FILES; k++) {

                    if (open_files[k].index_type == 0 && open_files[k].which_primary == pindexDesc) {  // only for secondary index files with corresponding primary index file...

                        SHT_SecondaryUpdateEntry(open_files[k].fd, updateArray, updateArraySize);
                    }
                }
                open_files[pindexDesc].split = 0;
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
            printf("Inserting secondary record with index_key = %s, tupleId = %d\n", srecord.index_key, srecord.tupleId);
            CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc, srecord));

            printf("\n");
        }

        printf("\n");

        // Print entries
        printf("\nPRINT ENTRIES FROM PRIMARY INDEX FILE\n");
        for (int i = 0; i < 150; i++) {
            printf("-");
        }
        printf("\n");

        int id = rand() % NO_RECORDS;
        printf("- For id %d :\n", id);
        CALL_OR_DIE(HT_PrintAllEntries(pindexDesc, &id));	
       
        printf("\n- For all entries :\n");
        CALL_OR_DIE(HT_PrintAllEntries(pindexDesc, NULL));
        printf("\n");

        printf("\nPRINT ENTRIES FROM SECONDARY INDEX FILE %s\n", sfiles[f]);
        for (int i = 0; i < 150; i++) {
            printf("-");
        }
        printf("\n");

        printf("\n- For all entries :\n");
        CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc, NULL));
        printf("\n");

        char key[30];
        if (strcmp(INDEX_KEY, "city") == 0) {
            r = rand() % NO_CITIES;
            memcpy(&key, cities[r], sizeof(char)*(strlen(cities[r])+1)); 
        }
        else if (strcmp(INDEX_KEY, "surname") == 0){
            r = rand() % NO_SURNAMES;
            memcpy(&key, surnames[r], sizeof(char)*(strlen(surnames[r])+1)); 
        }
        else {
            fprintf(stderr, "not available index_key\n");
            return HT_ERROR;
        }
        printf("- For %s = %s\n", INDEX_KEY, key);
        CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc, key));
        
        printf("\n");
    }

    int r;
    char key[30];
    if (strcmp(INDEX_KEY, "city") == 0) {
        r = rand() % NO_CITIES;
        memcpy(&key, cities[r], sizeof(char)*(strlen(cities[r])+1)); 
    }
    else if (strcmp(INDEX_KEY, "surname") == 0){
        r = rand() % NO_SURNAMES;
        memcpy(&key, surnames[r], sizeof(char)*(strlen(surnames[r])+1)); 
    }
    else {
        fprintf(stderr, "not available index_key\n");
        return HT_ERROR;
    }
  
    printf("\nRUN INNER JOIN FOR SPECIFIC INDEX_KEY\n");
    for (int i = 0; i < 100; i++) {
        printf("-");
    }
    printf("\n");
    
    printf("\nRECORDS FROM FIRST FILE WITH INDEX_KEY %s\n", key);
    for (int i = 0; i < 100; i++) {
        printf("-");
    }
    printf("\n");

    // print secondary indexes of 2 hash files 
    CALL_OR_DIE(SHT_PrintAllEntries(sindexes[0], key));
    printf("\n");
    
    printf("\nRECORDS FROM SECOND FILE WITH INDEX_KEY %s\n", key);
    for (int i = 0; i < 100; i++) {
        printf("-");
    }
    printf("\n");

    CALL_OR_DIE(SHT_PrintAllEntries(sindexes[1], key));
    printf("\n");
    
    // Test InnerJoin on random index_key = temp
    printf("\nINNER JOIN RESULT\n");
    for (int i = 0; i < 150; i++){
        printf("-");
    }
    printf("\n");
    
    CALL_OR_DIE(SHT_InnerJoin(sindexes[0], sindexes[1], key));
    printf("\n");

    printf("\nRUN INNER JOIN FOR INDEX_KEY = NULL\n");
    for (int i = 0; i < 150; i++){
            printf("-");
        }
    printf("\n");
    
    CALL_OR_DIE(SHT_InnerJoin(sindexes[0], sindexes[1], NULL));

    for (int f = 0; f < 2; f++){
        printf("PRINT STATISTICS FOR PRIMARY INDEX FILE %s\n", pfiles[f]);
        for (int i=0; i < 100; i++){
            printf("-");
        }
        printf("\n");
        
        CALL_OR_DIE(HashStatistics(pfiles[f]));
        printf("\n");
        
        printf("PRINT STATISTICS FOR SECONDARY INDEX FILE %s\n", sfiles[f]);
        for (int i=0; i < 100; i++){
            printf("-");
        }
        printf("\n");

        CALL_OR_DIE(SHT_HashStatistics(sfiles[f]));
        
        printf("\nCLOSE HASH FILES\n");
        for (int i = 0; i < 50; i++) {
            printf("-");
        }
        printf("\n");

        CALL_OR_DIE(HT_CloseFile(pindexes[f]));
        printf("\n");

        CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexes[f]));
        printf("\n");
    }
   
    BF_Close();

    return 0;
}