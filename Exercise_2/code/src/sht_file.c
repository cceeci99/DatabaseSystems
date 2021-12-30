#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_file.h"

#define SECONDARY_BLOCK_CAP (int) (BF_BLOCK_SIZE - 2*sizeof(int) / sizeof(SecondaryRecord))	// max records per block for secondary index


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}


HT_ErrorCode SHT_Init() {
    //insert code here

    // initialize open_files array 
    for (int i = 0; i < MAX_OPEN_FILES; i++) {
        open_files[i].fd = -1;
        open_files[i].depth = -1;
        open_files[i].inserted = -1;
        open_files[i].no_buckets = -1;
        open_files[i].no_hash_blocks = -1;
        open_files[i].filename = NULL;

        //
        open_files[i].index_type = -1;
    
    }
    return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth, char *fileName) {
    //insert code here
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

    if (strcmp(attrName, "city") == 0){
        char index_key = 1;
        memcpy(metadata + metadata_size, &index_key, sizeof(char));
        metadata_size += sizeof(char);
    }
    else if (strcmp(attrName, "surname") == 0){
        char index_key = 0;
        memcpy(metadata + metadata_size, &index_key, sizeof(char));
        metadata_size += sizeof(char);
    }
    else{
        fprintf(stderr, "not available key index\n");
        return -1;
    }

    // // add to metadata, the fileName of the primary index
    // memcpy(metadata+metadata_size, fileName, strlen(fileName)*sizeof(char));
    // metadata_size += strlen(fileName)*sizeof(char);

    // // add to metadata, the attributeName for the index-key which will be used
    // memcpy(metadata+metadata_size, attrName, attrLength*sizeof(char));
    // metadata_size += attrLength*sizeof(char);

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
    //insert code here
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

    char index_key;
    memcpy(&index_key, metadata + HASH_ID_LEN*sizeof(char), sizeof(char));
    if (index_key){
        printf("hash file uses city as key\n");
    }
    else{
        printf("hash file uses surname as key\b");
    }

    
    // Fetch its depth and no_hash_blocks
	int depth, no_hash_blocks;
	memcpy(&depth, metadata + HASH_ID_LEN * sizeof(char) + sizeof(char), sizeof(int));
	memcpy(&no_hash_blocks, metadata + HASH_ID_LEN * sizeof(char) + sizeof(char) + 1 * sizeof(int), sizeof(int));

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

            //
            open_files[i].index_type = 0;       // secondary index
			*indexDesc = i;  // position in open_files array
			flag = 0;
		}
		i++;
	}

    return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
    //insert code here
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

    //    
    open_files[indexDesc].index_type = -1;
    return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc, SecondaryRecord record) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index_key) {
    //insert code here
    return HT_OK;
}
