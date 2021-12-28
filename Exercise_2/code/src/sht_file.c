#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_file.h"

// max records per block for secondary index
#define SECONDARY_BLOCK_CAP (int) (BF_BLOCK_SIZE - 2*sizeof(int) / sizeof(SecondaryRecord))	


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
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
    }
    return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth, char *fileName) {
    //insert code here
    printf("Creating a secondary index hash file with name %s, on 
    key %s, with corresponding primary index file %s\n", sfileName attrName, fileName);

    CALL_BF(BF_CreateFile(sfilename)); 

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

    BF_Block *hash_block, data_block;
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
    
    

    return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index-key) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename) {
    //insert code here
    return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index-key) {
    //insert code here
    return HT_OK;
}
