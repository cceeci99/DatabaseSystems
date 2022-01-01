#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht.h"

#include "sht_file.h"


#define BLOCK_CAP (int) ( (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record) ) // max records per block for primary index
#define SECONDARY_BLOCK_CAP (int) ( (BF_BLOCK_SIZE - 2*sizeof(int)) / sizeof(SecondaryRecord) )	// max records per block for secondary index


#define CALL_BF(call)       \
{                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}


static char* hash_function(const char* index_key){
    static int no_buckets = 2 << (MAX_DEPTH - 1);

    long sum = 0;
    long mul = 0;

    for (int i = 0; i < strlen(index_key); i++){
        mul = (i % 4 == 0) ? 1 : mul * 256;
        sum += index_key[i] * mul;
    }

    sum = (int) (abs(sum) % no_buckets);

    static char result[MAX_DEPTH+1];
    result[0] = '\0';

    for (int z = no_buckets/2; z > 0; z >>= 1){
        strcat(result, ((sum & z) == z) ? "1" : "0");
    }

    return result;
}


HT_ErrorCode SHT_Init() {

    // initialize open_files array 
    for (int i = 0; i < MAX_OPEN_FILES; i++) {
        open_files[i].fd = -1;
        open_files[i].depth = -1;
        open_files[i].inserted = -1;
        open_files[i].no_buckets = -1;
        open_files[i].no_hash_blocks = -1;
        open_files[i].filename = NULL;
        open_files[i].index_type = -1;
    
    }
    return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth, char *fileName) {
    printf("Creating a secondary index hash file with name %s, on key %s, with corresponding primary index file %s\n", sfileName, attrName, fileName);

    CALL_BF(BF_CreateFile(sfileName)); 

    int fd;
    CALL_BF(BF_OpenFile(sfileName, &fd));

    // Allocate metadata block
    BF_Block* metadata_block;
    BF_Block_Init(&metadata_block);
    CALL_BF(BF_AllocateBlock(fd, metadata_block));

    size_t metadata_size = 0;

    // initialize metadata block to 0
    char* metadata = BF_Block_GetData(metadata_block);
    memset(metadata + metadata_size, 0, BF_BLOCK_SIZE);

    // set the file descriptor in metadata block
    char* hash_id = "HashFile";
    memcpy(metadata, hash_id, HASH_ID_LEN * sizeof(char));
    metadata_size += HASH_ID_LEN*sizeof(char);

    // set the global depth of the hashfile
    memcpy(metadata+metadata_size, &depth, sizeof(int));
    metadata_size += sizeof(int);

    // Calculate blocks assigned to hash table
    int no_buckets = 2 << (depth - 1);  // 2 ^ depth
    int no_hash_blocks = (no_buckets > HASH_CAP) > 0 ? no_buckets / HASH_CAP : 1;

    memcpy(metadata+metadata_size, &no_hash_blocks, sizeof(int));
    metadata_size += sizeof(int);

    BF_Block *hash_block, *data_block;
    BF_Block_Init(&hash_block);
    BF_Block_Init(&data_block);

    int limit = no_hash_blocks > 1 ? HASH_CAP : no_buckets;

    for (int i=0; i<no_hash_blocks; i++){

        // Allocate and initialize new hash block to 0
        CALL_BF(BF_AllocateBlock(fd, hash_block));
        char* hash_data = BF_Block_GetData(hash_block);
        memset(hash_data, 0, BF_BLOCK_SIZE);

        int no_blocks;
        CALL_BF(BF_GetBlockCounter(fd, &no_blocks));
        int hash_block_id = no_blocks - 1;
        memcpy(metadata + metadata_size + i*sizeof(int), &hash_block_id, sizeof(int));

        for (int j=0; j<limit; j++){

            // Allocate new data block and update its local depth and number of records
            CALL_BF(BF_AllocateBlock(fd, data_block));
            char* data = BF_Block_GetData(data_block);

            // Data Block's structure:
            // 0-3 bytes -> local depth
            // 4-7 bytes -> number of entries
            // - rest are secondary records(entries) , a struct of {index-key, tupleID}

            int local_depth = depth;
            memcpy(data, &local_depth, sizeof(int));

            int no_entries;
            memcpy(data + 1*sizeof(int), &no_entries, sizeof(int));
            memset(data + 2*sizeof(int), 0, BF_BLOCK_SIZE - 2*sizeof(int));
            
            BF_Block_SetDirty(data_block);
            CALL_BF(BF_UnpinBlock(data_block));

            int no_file_blocks;
            CALL_BF(BF_GetBlockCounter(fd, &no_file_blocks));

            int new_data_block_id = no_file_blocks-1;
            memcpy(hash_data + j*sizeof(int), &new_data_block_id, sizeof(int));

        }

        BF_Block_SetDirty(hash_block);
        CALL_BF(BF_UnpinBlock(hash_block));
    }

    BF_Block_SetDirty(metadata_block);
    CALL_BF(BF_UnpinBlock(metadata_block));

    // Free BF_block pointers
    BF_Block_Destroy(&hash_block);
    BF_Block_Destroy(&data_block);
    BF_Block_Destroy(&metadata_block);
    
    // Close file - if we need it -> call HT_OpenIndex
    CALL_BF(BF_CloseFile(fd));

    return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc) {
    int fd;
    CALL_BF(BF_OpenFile(sfileName, &fd));

    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, 0, block));

    char test_hash[HASH_ID_LEN];
    
    char* metadata = BF_Block_GetData(block);
    memcpy(test_hash, metadata, HASH_ID_LEN*sizeof(char));

    if (strcmp(test_hash, "HashFile") != 0) {
		fprintf(stderr, "Error: this is not a valid hash file\n");
		return HT_ERROR;
	}

    // Fetch its depth and no_hash_blocks
	int depth, no_hash_blocks;
	memcpy(&depth, metadata + HASH_ID_LEN * sizeof(char), sizeof(int));
	memcpy(&no_hash_blocks, metadata + HASH_ID_LEN * sizeof(char) + 1*sizeof(int), sizeof(int));

	// Unpin metadata block - we just read from it
	CALL_BF(BF_UnpinBlock(block));
    
    // Update open_files with an entry of the recently opened hash file 
	int flag = 1, i = 0;
	while (flag && i < MAX_OPEN_FILES) {
		if (open_files[i].fd == -1) {  // Find first available position
			printf("Attaching secondary index hash file with name '%s', fd=%d, depth=%d, no_hash_blocks=%d to open_files[%d]\n", sfileName, fd, depth, no_hash_blocks, i);
			open_files[i].fd = fd;
			open_files[i].depth = depth;
			open_files[i].inserted = 0;
			open_files[i].no_buckets = 2 << (depth - 1);
			open_files[i].no_hash_blocks = no_hash_blocks;
			open_files[i].filename = sfileName;
            open_files[i].index_type = 0;       // secondary index
			*indexDesc = i;  // position in open_files array
			flag = 0;
		}
		i++;
	}

    return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
    if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES) {
		fprintf(stderr, "Error: index out of bounds\n");
		return HT_ERROR;
	}

	printf("\nClosing hash file in position %d with name '%s' and fd=%d\n", indexDesc, open_files[indexDesc].filename, open_files[indexDesc].fd);
	CALL_BF(BF_CloseFile(open_files[indexDesc].fd));

	// Update open_files array
	open_files[indexDesc].fd = -1;
	open_files[indexDesc].depth = -1;
	open_files[indexDesc].inserted = -1;
	open_files[indexDesc].no_buckets = -1;
	open_files[indexDesc].no_hash_blocks = -1;
	open_files[indexDesc].filename = NULL;
    open_files[indexDesc].index_type = -1;
    return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc, SecondaryRecord record) {
    if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES){
        fprintf(stderr, "Error: index out of bounds\n");
        return HT_ERROR;
    }

    char* byte_string = hash_function(record.index_key);

    char* msb = malloc((open_files[indexDesc].depth + 1)*sizeof(char));
    for (int i = 0; i < open_files[indexDesc].depth; i++){
        msb[i] = byte_string[i];
    }

    msb[open_files[indexDesc].depth] = '\0';
    
    char* tmp;
    int bucket = strtol(msb, &tmp, 2);
    free(msb);

    int hash_block_index = bucket / HASH_CAP;
    int hash_block_pos = bucket % HASH_CAP;

    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
    char* metadata = BF_Block_GetData(block);

    int actual_hash_block_id;
    size_t size = HASH_ID_LEN * sizeof(char) + 2*sizeof(int);
    memcpy(&actual_hash_block_id, metadata + size + hash_block_index*sizeof(int), sizeof(int));

    int no_hash_blocks = open_files[indexDesc].no_hash_blocks;
    
    // unpin metadata block
    CALL_BF(BF_UnpinBlock(block));

    // Fetch the specified hash block
    CALL_BF(BF_GetBlock(open_files[indexDesc].fd, actual_hash_block_id, block));
    char* hash_data = BF_Block_GetData(block);

    // Fetch the specified bucket
    int data_block_id;
    memcpy(&data_block_id, hash_data + hash_block_pos*sizeof(int), sizeof(int));

    // unpin hash block
    CALL_BF(BF_UnpinBlock(block));

    BF_Block* data_block;
    BF_Block_Init(&data_block);

    CALL_BF(BF_GetBlock(open_files[indexDesc].fd, data_block_id, data_block));
    char* data = BF_Block_GetData(data_block);

    int local_depth;
    memcpy(&local_depth, data, sizeof(int));
    
    int no_records;
    memcpy(&no_records, data + sizeof(int), sizeof(int));

    int created_new_blocks = 0;

    if (no_records < SECONDARY_BLOCK_CAP){

        size = 2*sizeof(int) + no_records*sizeof(SecondaryRecord);
        memcpy(data + size, &record, sizeof(SecondaryRecord));

        no_records++;
        memcpy(data + 1*sizeof(int), &no_records, sizeof(int));
        printf("Inserting secondary index record{%s , %d} on hash block %d, data block %d, record pos %d\n", record.index_key, record.tupleId, actual_hash_block_id, data_block_id, no_records);

        BF_Block_SetDirty(data_block);
        CALL_BF(BF_UnpinBlock(data_block));
    }

    return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray) {
    return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key) {
    if (sindexDesc < 0 || sindexDesc > MAX_OPEN_FILES) {
		fprintf(stderr, "Error: index out of bounds\n");
		return HT_ERROR;
	}

    if (index_key == NULL){
        printf("Printing all records of secondary index hash file\n");

        BF_Block* block;
        BF_Block_Init(&block);

        CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, 0, block));
        char* metadata = BF_Block_GetData(block);

        int no_hash_blocks = open_files[sindexDesc].no_hash_blocks;
        int* hash_block_ids = malloc(no_hash_blocks*sizeof(int));

        size_t size = HASH_ID_LEN*sizeof(char) + 2*sizeof(int);

        for (int i = 0; i < no_hash_blocks; i++){
            memcpy(&hash_block_ids[i], metadata + size + i*sizeof(int), sizeof(int));
        }

        CALL_BF(BF_UnpinBlock(block));

        BF_Block* data_block;
        BF_Block_Init(&data_block);

        int current_id = 0;
        int counter = 0;

        for (int i = 0; i < no_hash_blocks; i ++){
            CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, hash_block_ids[i], block));
            char* hash_data = BF_Block_GetData(block);

            int limit = no_hash_blocks == 1 ? open_files[sindexDesc].no_buckets : HASH_CAP;

            for (int j = 0; j < limit; j++){
                int data_block_id;
                memcpy(&data_block_id, hash_data + j*sizeof(int), sizeof(int));

                // To avoid printing same records because of buddies
				if (current_id != data_block_id) {
					current_id = data_block_id;
				}
				else {
					continue;
				}

                CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, data_block_id, data_block));
                char* data = BF_Block_GetData(data_block);

                int no_records;
                memcpy(&no_records, data + 1*sizeof(int), sizeof(int));

                counter += no_records;

                SecondaryRecord record;
                size = 2*sizeof(int);

                for (int k = 0; k < no_records; k++){
                    memcpy(&record, data + size + k*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

                    printf("Record {index_key=%s, tupleId=%d}\n", record.index_key, record.tupleId);

                    BF_Block* b;
                    BF_Block_Init(&b);

                    int block_pos = record.tupleId / BLOCK_CAP;
                    int record_pos = record.tupleId % BLOCK_CAP;

                    printf("Wanted record is on block=%d and position=%d\n", block_pos, record_pos);

                    CALL_BF(BF_GetBlock(open_files[0].fd, block_pos, b));
                    char* d = BF_Block_GetData(b);

                    Record r;
                    memcpy(&r, d + 2*sizeof(int) + (record_pos-1)*sizeof(Record), sizeof(Record));

                    printf("id = %d , name = %s , surname = %s , city = %s \n", r.id, r.name, r.surname, r.city);
                    
                    CALL_BF(BF_UnpinBlock(b));
                }
                CALL_BF(BF_UnpinBlock(data_block));
            }
            CALL_BF(BF_UnpinBlock(block));
        }

        free(hash_block_ids);

        printf("\nTotal number of records : %d\n", counter);
        
	    BF_Block_Destroy(&block);
	    BF_Block_Destroy(&data_block);

    }
    else{
        printf("Searching record with index key= %s\n", index_key);

        char* byte_string = hash_function(index_key);

        char* msb = malloc((open_files[sindexDesc].depth + 1)*sizeof(char));
        for (int i = 0; i < open_files[sindexDesc].depth; i++){
            msb[i] = byte_string[i];
        }

        msb[open_files[sindexDesc].depth] = '\0';
        
        char* tmp;
        int bucket = strtol(msb, &tmp, 2);
        free(msb);

        int hash_block_index = bucket / HASH_CAP;
        int hash_block_pos = bucket % HASH_CAP;

        BF_Block* block;
        BF_Block_Init(&block);

        CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, 0, block));
        char* metadata = BF_Block_GetData(block);

        int actual_hash_block_id;
        size_t size = HASH_ID_LEN*sizeof(char) + 2*sizeof(int);
        memcpy(&actual_hash_block_id, metadata + size + hash_block_index*sizeof(int), sizeof(int));

        CALL_BF(BF_UnpinBlock(block));

        CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, actual_hash_block_id, block));
        char* hash_data = BF_Block_GetData(block);

        int data_block_id;
        memcpy(&data_block_id, hash_data + hash_block_pos*sizeof(int), sizeof(int));

        BF_Block* data_block;
        BF_Block_Init(&data_block);

        CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, data_block_id, data_block));
        char* data = BF_Block_GetData(data_block);

        int no_records;
        memcpy(&no_records, data + 1*sizeof(int), sizeof(int));

        SecondaryRecord record;
        size = 2*sizeof(int);
        int counter = 0;
        for (int k = 0; k < no_records ; k++){
            // search through secondary index record, those matching the index_key
            // and then get the corresponding record from the tupleId
            
            memcpy(&record, data + size + k*sizeof(SecondaryRecord), sizeof(SecondaryRecord));
            
            if (strcmp(index_key, record.index_key) == 0){
                printf("Index_key=%s, TupleId = %d\n", record.index_key, record.tupleId);

                BF_Block* b;
                BF_Block_Init(&b);

                int block_pos = record.tupleId / BLOCK_CAP; // get in which data_block is the record
                int record_pos = record.tupleId % BLOCK_CAP;    // get the position of record in the data_block

                printf("Wanted record is on block=%d and position=%d\n", block_pos, record_pos);

                CALL_BF(BF_GetBlock(open_files[0].fd, block_pos, b));
                char* d = BF_Block_GetData(b);

                Record r;
                memcpy(&r, d + size + (record_pos-1)*sizeof(Record), sizeof(Record));

                printf("id = %d , name = %s , surname = %s , city = %s \n", r.id, r.name, r.surname, r.city);
                counter++;

                CALL_BF(BF_UnpinBlock(b));
            }
        }

        if (counter == 0){
            printf("No records found with index_key=%s\n", index_key);
        }
        
        CALL_BF(BF_UnpinBlock(data_block));
        CALL_BF(BF_UnpinBlock(block));

        BF_Block_Destroy(&block);
        BF_Block_Destroy(&data_block);
    }

    return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename) {
    int fd;
	CALL_BF(BF_OpenFile(filename, &fd));

	// Access file's metadata block
	BF_Block* block;
	BF_Block_Init(&block);
	CALL_BF(BF_GetBlock(fd, 0, block));
	
	// Ensure it is a valid hash file and fetch its depth and its no_hash_blocks
	char test_hash[HASH_ID_LEN];

	char* metadata = BF_Block_GetData(block);
	memcpy(test_hash, metadata, HASH_ID_LEN * sizeof(char));
	if (strcmp(test_hash, "HashFile") != 0) {
		fprintf(stderr, "Error: this is not a valid hash file\n");
		return HT_ERROR;
	}

	// Fetch its depth and no_hash_blocks
	int depth, no_hash_blocks;
	memcpy(&depth, metadata + HASH_ID_LEN * sizeof(char), sizeof(int));
	memcpy(&no_hash_blocks, metadata + HASH_ID_LEN * sizeof(char) + 1 * sizeof(int), sizeof(int));

	int no_buckets = 2 << (depth - 1);

	int* hash_block_ids = malloc(no_hash_blocks * sizeof(int));

	size_t sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int);
	for (int i = 0; i < no_hash_blocks; i++) {
		memcpy(&hash_block_ids[i], metadata + sz + i * sizeof(int), sizeof(int));
	}

	// Unpin metadata block - we just read from it
	CALL_BF(BF_UnpinBlock(block));

	BF_Block* data_block;
	BF_Block_Init(&data_block);
	
	int current_id = 0;
	for (int i = 0; i < no_hash_blocks; i++) {
		
		int counter = 0;

		int min_no_records = SECONDARY_BLOCK_CAP;
		int max_no_records = 0;
		
		CALL_BF(BF_GetBlock(fd, hash_block_ids[i], block));
		char* hash_data = BF_Block_GetData(block);

		int limit = no_hash_blocks == 1 ? no_buckets : HASH_CAP;

		int no_data_blocks = 0;

		for (int j = 0; j < limit; j++) {
			int data_block_id;
			memcpy(&data_block_id, hash_data + j * sizeof(int), sizeof(int));

			// To avoid printing same records because of buddies
			if (current_id != data_block_id) {
				current_id = data_block_id;
				no_data_blocks++;
			}
			else {
				continue;
			}

			CALL_BF(BF_GetBlock(fd, data_block_id, data_block));
			char* data = BF_Block_GetData(data_block);

			int no_records;
			memcpy(&no_records, data + 1 * sizeof(int), sizeof(int));

			// Update min & max no_records
			if (no_records < min_no_records) {
				min_no_records = no_records;
			}
			if (no_records > max_no_records) {
				max_no_records = no_records;
			}
			counter += no_records;

			// Unpin data block
			CALL_BF(BF_UnpinBlock(data_block));
		}
		// Unpin hash block
		CALL_BF(BF_UnpinBlock(block));

		int avg_no_records = counter / no_data_blocks;

		printf("Hash block %d :\n", i+1);
		printf("- min numbers of records: %d\n", min_no_records);
		printf("- avg numbers of records: %d\n", avg_no_records);
		printf("- max numbers of records: %d\n", max_no_records);
		printf("\n\n");
	}

	free(hash_block_ids);

	BF_Block_Destroy(&block);
	BF_Block_Destroy(&data_block);
    return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index_key) {
    return HT_OK;
}
