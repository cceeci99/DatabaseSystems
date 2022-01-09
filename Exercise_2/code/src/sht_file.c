#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht.h"

#include "sht_file.h"


#define BLOCK_CAP (int) ( (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record) ) // max records per block for primary index

// ο αριθμός των εγγραφών που χωράει το δευτερεύον ευρετήριο αλλάζει καθώς αλλάζει η δομη της εγγραφής που αποθηκεύεται.
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

	char which_index_key;

	// // add a single char to define which index key is used for hashing, c for city, s for surname.
	if (strcmp(attrName, "city")==0){
		which_index_key = 'c';
	}
	else if (strcmp(attrName, "surname")==0){
		which_index_key = 's';
	}
	else{
		fprintf(stderr, "Not available index_key\n");
		return HT_ERROR;
	}

	memcpy(metadata + metadata_size, &which_index_key, 1*sizeof(char));
	metadata_size += 1*sizeof(char);

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

    for (int i = 0; i < no_hash_blocks; i++) {

        // Allocate and initialize new hash block to 0
        CALL_BF(BF_AllocateBlock(fd, hash_block));
        char* hash_data = BF_Block_GetData(hash_block);
        memset(hash_data, 0, BF_BLOCK_SIZE);

        int no_blocks;
        CALL_BF(BF_GetBlockCounter(fd, &no_blocks));
        int hash_block_id = no_blocks - 1;
        memcpy(metadata + metadata_size + i*sizeof(int), &hash_block_id, sizeof(int));

        for (int j = 0; j < limit; j++) {

            // Allocate new data block and update its local depth and number of records
            CALL_BF(BF_AllocateBlock(fd, data_block));
            char* data = BF_Block_GetData(data_block);

            // Data Block's structure:
            // 0-3 bytes -> local depth
            // 4-7 bytes -> number of entries
            // - rest are secondary records(entries) , a struct of {index-key, tupleID}

            int local_depth = depth;
            memcpy(data, &local_depth, sizeof(int));

            int no_records;
            memcpy(data + 1*sizeof(int), &no_records, sizeof(int));
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

	size_t metadata_size = HASH_ID_LEN * sizeof(char);

    if (strcmp(test_hash, "HashFile") != 0) {
		fprintf(stderr, "Error: this is not a valid hash file\n");
		return HT_ERROR;
	}
	
	char which_index_key;
	memcpy(&which_index_key, metadata + metadata_size, sizeof(char));
	metadata_size += 1 * sizeof(char);

    // Fetch its depth and no_hash_blocks
	int depth;
	memcpy(&depth, metadata + metadata_size, sizeof(int));
	metadata_size += 1 * sizeof(int);

	int no_hash_blocks;
	memcpy(&no_hash_blocks, metadata + metadata_size, sizeof(int));

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
			open_files[i].which_index_key = which_index_key;
			
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
	open_files[indexDesc].which_index_key = '\0';

    return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc, SecondaryRecord record) {
    if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES) {
        fprintf(stderr, "Error: index out of bounds\n");
        return HT_ERROR;
    }

    char* byte_string = hash_function(record.index_key);

    char* msb = malloc((open_files[indexDesc].depth + 1)*sizeof(char));
    for (int i = 0; i < open_files[indexDesc].depth; i++) {
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
    size_t size = HASH_ID_LEN * sizeof(char) + 1*sizeof(char) + 2*sizeof(int);
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
    if (no_records < SECONDARY_BLOCK_CAP) {
		
        size = 2*sizeof(int) + no_records*sizeof(SecondaryRecord);
        memcpy(data + size, &record, sizeof(SecondaryRecord));

        no_records++;
        memcpy(data + 1*sizeof(int), &no_records, sizeof(int));

        BF_Block_SetDirty(data_block);
        CALL_BF(BF_UnpinBlock(data_block));
    }
    	// Case 2.2: the data block has not enough space to store the given record
	else {
		// Case 2.2.1: local depth == global depth
		if (local_depth == open_files[indexDesc].depth) {
			// Save old depth and no_buckets
			int old_depth = open_files[indexDesc].depth;
			int old_no_buckets = open_files[indexDesc].no_buckets;

			// Update depth and no_buckets
			open_files[indexDesc].depth += 1;
			open_files[indexDesc].no_buckets *= 2;

			if (open_files[indexDesc].depth > 13) {
				fprintf(stderr, "Error: maximum depth is %d", MAX_DEPTH);
				return HT_ERROR;
			}

			// Case : we need more hash blocks
			int current_cap = no_hash_blocks * HASH_CAP;
			if (open_files[indexDesc].no_buckets > current_cap) {

				created_new_blocks = 1;	// update flag

				int no_new_hash_blocks_needed = (open_files[indexDesc].no_buckets - current_cap) / HASH_CAP;

				for (int i = 0; i < no_new_hash_blocks_needed; i++) {

					// Allocate new hash block and sets is content to 0
					CALL_BF(BF_AllocateBlock(open_files[indexDesc].fd, block));
					char* new_hash_block_data = BF_Block_GetData(block);
					memset(new_hash_block_data, 0, BF_BLOCK_SIZE);

					// We changed the new hash block -> set dirty & unpin
					BF_Block_SetDirty(block);
					CALL_BF(BF_UnpinBlock(block));

					// Update the metadata block
					int no_blocks;
					CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fd, &no_blocks));
					int new_hash_block_id = no_blocks - 1;

					CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
					metadata = BF_Block_GetData(block);

					int old_no_hash_blocks;
					int sz = HASH_ID_LEN * sizeof(char) + 1*sizeof(char) + 1 * sizeof(int);
					memcpy(&old_no_hash_blocks, metadata + sz, sizeof(int));

					sz = HASH_ID_LEN * sizeof(char) + 1*sizeof(char) + 2 * sizeof(int) + old_no_hash_blocks * sizeof(int);
					memcpy(metadata + sz, &new_hash_block_id, sizeof(int));

					int new_no_hash_blocks = old_no_hash_blocks + 1;
					sz = HASH_ID_LEN * sizeof(char) + 1*sizeof(char) + 1 * sizeof(int);
					memcpy(metadata + sz, &new_no_hash_blocks, sizeof(int));

					// We changed the metadata block -> set dirty & unpin
					BF_Block_SetDirty(block);
					CALL_BF(BF_UnpinBlock(block));

				}

				int old_no_hash_blocks = open_files[indexDesc].no_hash_blocks;

				open_files[indexDesc].no_hash_blocks += no_new_hash_blocks_needed;
				int new_no_hash_blocks = open_files[indexDesc].no_hash_blocks;

				CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
				metadata = BF_Block_GetData(block);
				
				// Save the ids of the the hash blocks
				int* hash_block_ids = malloc(new_no_hash_blocks * sizeof(int));
				
				int sz = HASH_ID_LEN * sizeof(char) + 1*sizeof(char) + 2 * sizeof(int);
				for (int i = 0; i < new_no_hash_blocks; i++) {
					memcpy(&hash_block_ids[i], metadata + sz + i * sizeof(int), sizeof(int));
				}

				// Unpin metadata block
				CALL_BF(BF_UnpinBlock(block));

				int temp;

				// Save the ids of the data blocks
				int* data_block_ids = malloc(old_no_buckets * sizeof(int));

				for (int i = 0; i < old_no_hash_blocks; i++) {

					CALL_BF(BF_GetBlock(open_files[indexDesc].fd, hash_block_ids[i], block));
					hash_data = BF_Block_GetData(block);

					for (int j = 0; j < HASH_CAP; j++) {
						temp = j + i * HASH_CAP;
						memcpy(&data_block_ids[temp], hash_data + j * sizeof(int), sizeof(int));
					}

					// Unpin hash block
					CALL_BF(BF_UnpinBlock(block));
				}

				// Update the buckets of each hash block to point to the appropriate data block id
				for (int i = 0; i < new_no_hash_blocks; i++) {

					CALL_BF(BF_GetBlock(open_files[indexDesc].fd, hash_block_ids[i], block));
					hash_data = BF_Block_GetData(block);

					for (int j = 0; j < HASH_CAP; j+=2) {
						temp = j + i * HASH_CAP;
						memcpy(hash_data + j * sizeof(int), &data_block_ids[temp / 2], sizeof(int));
						memcpy(hash_data + (j + 1) * sizeof(int), &data_block_ids[temp / 2], sizeof(int));
					}

					// We changed the hash block -> set dirty & unpin
					BF_Block_SetDirty(block);
					CALL_BF(BF_UnpinBlock(block));
				}

				free(hash_block_ids);
				free(data_block_ids);
			}
			// We don't need new hash block so we just modify the current one
			else {

				CALL_BF(BF_GetBlock(open_files[indexDesc].fd, actual_hash_block_id, block));
				hash_data = BF_Block_GetData(block);
				
				int* data_block_ids = malloc(old_no_buckets * sizeof(int));
				for (int i = 0; i < old_no_buckets; i++) {
					memcpy(&data_block_ids[i], hash_data + i * sizeof(int), sizeof(int));
				}

				for (int i = 0; i < open_files[indexDesc].no_buckets; i+=2) {
					memcpy(hash_data + i * sizeof(int), &data_block_ids[i / 2], sizeof(int));
					memcpy(hash_data + (i + 1) * sizeof(int), &data_block_ids[i / 2], sizeof(int));
				}

				free(data_block_ids);

				// We changed the hash block -> set dirty & unpin
				BF_Block_SetDirty(block);
				CALL_BF(BF_UnpinBlock(block));
			}

			// Update depth in metadata block
			CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
			metadata = BF_Block_GetData(block);

			size_t metadata_size;
			metadata_size = HASH_ID_LEN * sizeof(char) + 1 * sizeof(char);
			memcpy(metadata + metadata_size, &open_files[indexDesc].depth, sizeof(int));

			// We changed metadata block -> set dirty & unpin
			BF_Block_SetDirty(block);
			CALL_BF(BF_UnpinBlock(block));
		}

		// If we created new hash blocks we need to rehash the record.id to find in which hash block its data block id is stored
		if (created_new_blocks == 1) {

			// Get the hashed value of the id
			char* new_byte_string = hash_function(record.index_key);
			
			// Get depth MSB of hashed value
			char* new_msb = malloc((open_files[indexDesc].depth + 1) * sizeof(char));
			for (int i = 0; i < open_files[indexDesc].depth; i++) {
				new_msb[i] = byte_string[i];
			}
			new_msb[open_files[indexDesc].depth] = '\0';

			// Get the bucket based on the MSB
			char* new_tmp;
			bucket = strtol(new_msb, &new_tmp, 2);
			free(new_msb);

			hash_block_index = bucket / HASH_CAP;  // in which hash block is the bucket
			hash_block_pos = bucket % HASH_CAP;    // which is the position of the bucket in that hash block

			CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
			metadata = BF_Block_GetData(block);

			int sz = HASH_ID_LEN * sizeof(char) + 1*sizeof(char) + 2 * sizeof(int);
			memcpy(&actual_hash_block_id, metadata + sz + hash_block_index * sizeof(int), sizeof(int));

			// Unpin metadata block
			CALL_BF(BF_UnpinBlock(block));
		}

		// Case : local depth < global depth

		CALL_BF(BF_GetBlock(open_files[indexDesc].fd, actual_hash_block_id, block));
		hash_data = BF_Block_GetData(block);

		BF_Block* new_data_block;
		BF_Block_Init(&new_data_block);
		CALL_BF(BF_AllocateBlock(open_files[indexDesc].fd, new_data_block));

		int new_no_blocks;
		CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fd, &new_no_blocks));
		int new_data_block_id = new_no_blocks - 1;
		int old_data_block_id = data_block_id;

		char* new_data_block_data = BF_Block_GetData(new_data_block);
		char* old_data_block_data = data;

		// Update local depths (d' = d' + 1)
		int new_local_depth = local_depth + 1;
		memcpy(old_data_block_data, &new_local_depth, sizeof(int));
		memcpy(new_data_block_data, &new_local_depth, sizeof(int));

		// Set the two data blocks no_records to 0
		int refresh_no_records = 0;
		memcpy(old_data_block_data + 1 * sizeof(int), &refresh_no_records, sizeof(int));
		memcpy(new_data_block_data + 1 * sizeof(int), &refresh_no_records, sizeof(int));

		// Find the buddies which point to the same data block where record.id was supposed to get stored
		int db_id, first_bucket;
		int set_first_bucket = 0, no_buddies = 0, flag = 0;
		for (int i = 0; i < open_files[indexDesc].no_buckets; i++) {
			memcpy(&db_id, hash_data + i * sizeof(int), sizeof(int));
			if (db_id == old_data_block_id) {
				no_buddies++;
				flag = 1;
				if (set_first_bucket == 0) {
					first_bucket = i;
					set_first_bucket = 1;
				}
			}
			if (db_id != old_data_block_id && flag == 1) {	// no need to search anymore - we have found all the buddies
				break;
			}
		}

		// Update buckets so first half point to the old data block and second half to the new data block
		for (int i = 0; i < no_buddies / 2; i++) {
			memcpy(hash_data + (first_bucket + i) * sizeof(int), &old_data_block_id, sizeof(int));
		}

		for (int i = no_buddies / 2; i < no_buddies; i++) {
			memcpy(hash_data + (first_bucket + i) * sizeof(int), &new_data_block_id, sizeof(int));
		}

		// Save the records that were stored in the old data block
		SecondaryRecord* records = malloc((no_records + 1) * sizeof(SecondaryRecord));
		for (int i = 0; i < no_records; i++) {
			int sz = 2 * sizeof(int) + i * sizeof(SecondaryRecord);
			memcpy(&records[i], old_data_block_data + sz, sizeof(SecondaryRecord));
		}
		records[no_records] = record; // new record to be inserted

		// Reinitialize all records to 0
		memset(old_data_block_data + 2 * sizeof(int), 0, BF_BLOCK_SIZE - 2 * sizeof(int));
		memset(new_data_block_data + 2 * sizeof(int), 0, BF_BLOCK_SIZE - 2 * sizeof(int));

		// Cleanup before we re-insert each record
		BF_Block_SetDirty(block);
		CALL_BF(BF_UnpinBlock(block));
		BF_Block_Destroy(&block);

		BF_Block_SetDirty(data_block);
		CALL_BF(BF_UnpinBlock(data_block));
		BF_Block_Destroy(&data_block);
		
		BF_Block_SetDirty(new_data_block);
		CALL_BF(BF_UnpinBlock(new_data_block));
		BF_Block_Destroy(&new_data_block);

		// Insert again all records
		for (int i = 0; i < no_records + 1; i++) {
			if (SHT_SecondaryInsertEntry(indexDesc, records[i]) == HT_ERROR) {
				return HT_ERROR;
			}
			open_files[indexDesc].inserted--;  // avoid calculating same entry many times
		}
		free(records);

	}

    return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray, int updateArraySize) {
	// for each record that hash changed find it in the secondary index hash file and update it's tupleId

 	if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES) {
        fprintf(stderr, "Error: index out of bounds\n");
        return HT_ERROR;
    }

	for (int i = 0; i < updateArraySize; i++) {
    
		char* index_key;

		if ( open_files[indexDesc].which_index_key == 'c'){
			// hash on city
			index_key = updateArray[i].city;
		}
		else {
			// hash on surname
			index_key = updateArray[i].surname;
		}

		char* byte_string = hash_function(index_key);

        char* msb = malloc((open_files[indexDesc].depth + 1)*sizeof(char));
        for (int i = 0; i < open_files[indexDesc].depth; i++) {
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
        size_t size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);
        memcpy(&actual_hash_block_id, metadata + size + hash_block_index*sizeof(int), sizeof(int));

        CALL_BF(BF_UnpinBlock(block));

        CALL_BF(BF_GetBlock(open_files[indexDesc].fd, actual_hash_block_id, block));
        char* hash_data = BF_Block_GetData(block);

        int data_block_id;
        memcpy(&data_block_id, hash_data + hash_block_pos*sizeof(int), sizeof(int));

        BF_Block* data_block;
        BF_Block_Init(&data_block);

        CALL_BF(BF_GetBlock(open_files[indexDesc].fd, data_block_id, data_block));
        char* data = BF_Block_GetData(data_block);

        int no_records;
        memcpy(&no_records, data + 1*sizeof(int), sizeof(int));

        SecondaryRecord record;
        size = 2*sizeof(int);
		
		for (int j = 0; j < no_records; j++){
			
			memcpy(&record, data + size + j*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

			// linear search, untill you find matching record, on index_key and oldTupleId, and update if it's changed
			if ( (strcmp(index_key, record.index_key) == 0) && (updateArray[i].oldTupleId == record.tupleId) ) {
				
				if (record.tupleId != updateArray[i].newTupleId) {
					// update the record's tupleId
					record.tupleId = updateArray[i].newTupleId;

					// write it back to the data_block
					memcpy(data+size+j*sizeof(SecondaryRecord), &record, sizeof(SecondaryRecord));
					
					BF_Block_SetDirty(data_block);
				}
				break;
			}		

		}
		CALL_BF(BF_UnpinBlock(data_block));

		CALL_BF(BF_UnpinBlock(block));

		BF_Block_Destroy(&block);
	    BF_Block_Destroy(&data_block);
    }	

    return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key) {
    
	if (sindexDesc < 0 || sindexDesc > MAX_OPEN_FILES) {
		fprintf(stderr, "Error: index out of bounds\n");
		return HT_ERROR;
	}

	char* which_key;
	if (open_files[sindexDesc].which_index_key == 'c') {
		which_key = "city";
	}
	else{
		which_key = "surname";
	}
    
	if (index_key == NULL) {
	
        printf("Printing all records from secondary index hash file %s with index_key = %s\n", open_files[sindexDesc].filename, which_key);

        BF_Block* block;
        BF_Block_Init(&block);

        CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, 0, block));
        char* metadata = BF_Block_GetData(block);

        int no_hash_blocks = open_files[sindexDesc].no_hash_blocks;
        int* hash_block_ids = malloc(no_hash_blocks*sizeof(int));

        size_t size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);

        for (int i = 0; i < no_hash_blocks; i++) {
            memcpy(&hash_block_ids[i], metadata + size + i*sizeof(int), sizeof(int));
        }

        CALL_BF(BF_UnpinBlock(block));

        BF_Block* data_block;
        BF_Block_Init(&data_block);

        int current_id = 0;
        int counter = 0;

        for (int i = 0; i < no_hash_blocks; i ++) {
            CALL_BF(BF_GetBlock(open_files[sindexDesc].fd, hash_block_ids[i], block));
            char* hash_data = BF_Block_GetData(block);

            int limit = no_hash_blocks == 1 ? open_files[sindexDesc].no_buckets : HASH_CAP;

            for (int j = 0; j < limit; j++) {
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

                for (int k = 0; k < no_records; k++) {
                    memcpy(&record, data + size + k*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

                    printf("Record {index_key=%s, tupleId=%d}\n", record.index_key, record.tupleId);

                    BF_Block* b;
                    BF_Block_Init(&b);

                    int block_pos = record.tupleId / BLOCK_CAP;
                    int record_pos = record.tupleId % BLOCK_CAP;
					
					int primaryIndex = open_files[sindexDesc].which_primary;

					CALL_BF(BF_GetBlock(open_files[primaryIndex].fd, block_pos, b));
                    char* d = BF_Block_GetData(b);

                    Record r;
                    memcpy(&r, d + 2*sizeof(int) + record_pos*sizeof(Record), sizeof(Record));
				
					printf("id = %d , name = %s , surname = %s , city = %s \n", r.id, r.name, r.surname, r.city);
				
					printf("\n");

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
        printf("Printing records with index key: %s = %s from secondary index hash file %s\n", which_key, index_key, open_files[sindexDesc].filename);

        char* byte_string = hash_function(index_key);

        char* msb = malloc((open_files[sindexDesc].depth + 1)*sizeof(char));
        for (int i = 0; i < open_files[sindexDesc].depth; i++) {
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
        size_t size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);
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
        for (int k = 0; k < no_records ; k++) {
            // search through secondary index record, those matching the index_key
            // and then get the corresponding record from the tupleId
            
            memcpy(&record, data + size + k*sizeof(SecondaryRecord), sizeof(SecondaryRecord));
            
            if (strcmp(index_key, record.index_key) == 0) {
                printf("Record {index_key=%s, tupleId=%d}\n", record.index_key, record.tupleId);

                BF_Block* b;
                BF_Block_Init(&b);

                int block_pos = record.tupleId / BLOCK_CAP; // get in which data_block is the record
                int record_pos = record.tupleId % BLOCK_CAP;    // get the position of record in the data_block
				
				int primaryIndex = open_files[sindexDesc].which_primary;
               
			    CALL_BF(BF_GetBlock(open_files[primaryIndex].fd , block_pos, b));
                char* d = BF_Block_GetData(b);

                Record r;
                memcpy(&r, d + size + record_pos*sizeof(Record), sizeof(Record));
				
                printf("id = %d , name = %s , surname = %s , city = %s \n", r.id, r.name, r.surname, r.city);
			
                counter++;
				printf("\n");

                CALL_BF(BF_UnpinBlock(b));			
            }
        }

        if (counter == 0) {
            printf("No records found with %s = %s\n", which_key, index_key);
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

	printf("Printing Statistics for secondary index file %s\n", filename);

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

	size_t size = HASH_ID_LEN * sizeof(char) + 1 * sizeof(char);

	// Fetch its depth and no_hash_blocks
	int depth;
	memcpy(&depth, metadata + size, sizeof(int));
	size += sizeof(int);
	
	int no_hash_blocks;
	memcpy(&no_hash_blocks, metadata + size, sizeof(int));
	size += sizeof(int);

	int no_buckets = 2 << (depth - 1);

	int* hash_block_ids = malloc(no_hash_blocks * sizeof(int));

	for (int i = 0; i < no_hash_blocks; i++) {
		memcpy(&hash_block_ids[i], metadata + size + i * sizeof(int), sizeof(int));
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

		printf("Hash block %d :\n", i);
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
	
	// check first if secondary indexes are hashing on same index-key, if so return error
	if (open_files[sindexDesc1].which_index_key != open_files[sindexDesc2].which_index_key) {
		fprintf(stderr, "Secondary indexes must have same index_key");
		return HT_ERROR;
	}

	char* which_key;
	if (open_files[sindexDesc1].which_index_key == 'c') {
		which_key = "city";
	}
	else if (open_files[sindexDesc1].which_index_key == 's'){
		which_key = "surname";
	}
	else {
		fprintf(stderr, "not available index key\n");
		return HT_ERROR;
	}

	if (index_key != NULL) {
		printf("Inner join on %s = %s\n", which_key, index_key);
		int joins = 0;
		
		char* byte_string = hash_function(index_key);
		char* msb = malloc((open_files[sindexDesc1].depth + 1)*sizeof(char));

        for (int i = 0; i < open_files[sindexDesc1].depth; i++) {
            msb[i] = byte_string[i];
        }

		msb[open_files[sindexDesc1].depth] = '\0';

		char* tmp;
		int bucket = strtol(msb, &tmp, 2);
		free(msb);

		int hash_block_index = bucket / HASH_CAP;
		int hash_block_pos = bucket % HASH_CAP;

		BF_Block* block;
		BF_Block_Init(&block);

		CALL_BF(BF_GetBlock(open_files[sindexDesc1].fd, 0, block));
		char* metadata = BF_Block_GetData(block);

		int actual_hash_block_id;
		size_t size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);

		memcpy(&actual_hash_block_id, metadata + size + hash_block_index*sizeof(int), sizeof(int));

		CALL_BF(BF_UnpinBlock(block));

		CALL_BF(BF_GetBlock(open_files[sindexDesc1].fd, actual_hash_block_id, block));
		char* hash_data = BF_Block_GetData(block);

		int data_block_id;
		memcpy(&data_block_id, hash_data + hash_block_pos*sizeof(int), sizeof(int));

		BF_Block* data_block;
		BF_Block_Init(&data_block);

		CALL_BF(BF_GetBlock(open_files[sindexDesc1].fd, data_block_id, data_block));
		char* data = BF_Block_GetData(data_block);

		int no_records;
		memcpy(&no_records, data + 1*sizeof(int), sizeof(int));

		SecondaryRecord record;
		size = 2*sizeof(int);
		for (int i = 0; i < no_records; i++) {

			memcpy(&record, data + size + i * sizeof(SecondaryRecord), sizeof(SecondaryRecord));

			// find the record from the first secondary index
			if ( strcmp(index_key, record.index_key) == 0 ) {
				
				BF_Block* b;
                BF_Block_Init(&b);

                int block_pos = record.tupleId / BLOCK_CAP; // get in which data_block is the record
                int record_pos = record.tupleId % BLOCK_CAP;    // get the position of record in the data_block
				
				int p = open_files[sindexDesc1].which_primary;
				
			    CALL_BF(BF_GetBlock(open_files[p].fd , block_pos, b));
                char* d = BF_Block_GetData(b);

				// primary record of first hash file
				Record r;
                memcpy(&r, d + size + record_pos*sizeof(Record), sizeof(Record));
				
				// find secondary record from second file, by hashing the index_key
				char* byte_string = hash_function(index_key);
				char* msb = malloc((open_files[sindexDesc2].depth + 1)*sizeof(char));

				for (int i = 0; i < open_files[sindexDesc2].depth; i++) {
					msb[i] = byte_string[i];
				}

				msb[open_files[sindexDesc2].depth] = '\0';

				char* tmp;
				int bucket = strtol(msb, &tmp, 2);
				free(msb);

				int hash_block_index = bucket / HASH_CAP;
				int hash_block_pos = bucket % HASH_CAP;

				BF_Block* block2;
				BF_Block_Init(&block2);

				CALL_BF(BF_GetBlock(open_files[sindexDesc2].fd, 0, block2));
				char* metadata2 = BF_Block_GetData(block2);

				size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);

				memcpy(&actual_hash_block_id, metadata2 + size + hash_block_index*sizeof(int), sizeof(int));

				CALL_BF(BF_UnpinBlock(block2));

				CALL_BF(BF_GetBlock(open_files[sindexDesc2].fd, actual_hash_block_id, block2));
				char* hash_data2 = BF_Block_GetData(block2);

				memcpy(&data_block_id, hash_data2 + hash_block_pos*sizeof(int), sizeof(int));

				BF_Block* data_block2;
				BF_Block_Init(&data_block2);

				CALL_BF(BF_GetBlock(open_files[sindexDesc2].fd, data_block_id, data_block2));
				char* data2 = BF_Block_GetData(data_block2);

				int no_records2;
				memcpy(&no_records2, data2 + 1*sizeof(int), sizeof(int));

				SecondaryRecord record2;
				size = 2*sizeof(int);

				for (int i = 0; i < no_records2; i++) {

					memcpy(&record2, data2 + size + i * sizeof(SecondaryRecord), sizeof(SecondaryRecord));

					// find matching record from second hash file
					if ( strcmp(index_key, record2.index_key) == 0 ) {
						joins++;

						BF_Block* bb;
						BF_Block_Init(&bb);

						int block_pos = record2.tupleId / BLOCK_CAP; // get in which data_block is the record
						int record_pos = record2.tupleId % BLOCK_CAP;    // get the position of record in the data_block
						
						int p = open_files[sindexDesc2].which_primary;

						CALL_BF(BF_GetBlock(open_files[p].fd , block_pos, bb));
						char* d = BF_Block_GetData(bb);

						// primary record of second hash file
						Record rr;
						memcpy(&rr, d + size + record_pos*sizeof(Record), sizeof(Record));
						
						if (strcmp(which_key, "city") == 0){
							printf("%s | id = %d | name = %s | surname = %s | id = %d | name = %s | surname = %s |\n",index_key, r.id, r.name, r.city, rr.id, rr.name, rr.city);
						}
						else if (strcmp(which_key, "surname") == 0) {
							printf("%s | id = %d | name = %s | city = %s | id = %d | name = %s | city = %s |\n",index_key, r.id, r.name, r.city, rr.id, rr.name, rr.city);
						}
						else {
							fprintf(stderr, "not available index key\n");
							return HT_ERROR;
						}
					
						CALL_BF(BF_UnpinBlock(bb));
					}
				}

				CALL_BF(BF_UnpinBlock(data_block2));
				CALL_BF(BF_UnpinBlock(block2));

				BF_Block_Destroy(&data_block2);
				BF_Block_Destroy(&block2);

				CALL_BF(BF_UnpinBlock(b));
			}
		}

		if (joins == 0) {
			printf("Inner Join is empty\n");
		}

		CALL_BF(BF_UnpinBlock(data_block));
        CALL_BF(BF_UnpinBlock(block));

 		BF_Block_Destroy(&data_block);
        BF_Block_Destroy(&block);
	
	}
	else {

		// since index_key is NULL print the join for every one record of the first file
		BF_Block* block;
        BF_Block_Init(&block);

        CALL_BF(BF_GetBlock(open_files[sindexDesc1].fd, 0, block));
        char* metadata = BF_Block_GetData(block);

        int no_hash_blocks = open_files[sindexDesc1].no_hash_blocks;
        int* hash_block_ids = malloc(no_hash_blocks*sizeof(int));

        size_t size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);

        for (int i = 0; i < no_hash_blocks; i++) {
            memcpy(&hash_block_ids[i], metadata + size + i*sizeof(int), sizeof(int));
        }

        CALL_BF(BF_UnpinBlock(block));

        BF_Block* data_block;
        BF_Block_Init(&data_block);

        for (int i = 0; i < no_hash_blocks; i ++) {
            CALL_BF(BF_GetBlock(open_files[sindexDesc1].fd, hash_block_ids[i], block));
            char* hash_data = BF_Block_GetData(block);

            int limit = no_hash_blocks == 1 ? open_files[sindexDesc1].no_buckets : HASH_CAP;

            for (int j = 0; j < limit; j++) {
                int data_block_id;
                memcpy(&data_block_id, hash_data + j*sizeof(int), sizeof(int));

                CALL_BF(BF_GetBlock(open_files[sindexDesc1].fd, data_block_id, data_block));
                char* data = BF_Block_GetData(data_block);

                int no_records;
                memcpy(&no_records, data + 1*sizeof(int), sizeof(int));

                SecondaryRecord record;
                size = 2*sizeof(int);

                for (int k = 0; k < no_records; k++) {
					// take as index_key for join, each record from first hash file
					int joins = 0;

                    memcpy(&record, data + size + k*sizeof(SecondaryRecord), sizeof(SecondaryRecord));
					printf("Inner join on index_key %s = %s\n", which_key, record.index_key);

                    BF_Block* b;
                    BF_Block_Init(&b);

                    int block_pos = record.tupleId / BLOCK_CAP;
                    int record_pos = record.tupleId % BLOCK_CAP;
					
					int p = open_files[sindexDesc1].which_primary;

					CALL_BF(BF_GetBlock(open_files[p].fd, block_pos, b));
                    char* d = BF_Block_GetData(b);

					// primary record of first hash file
                    Record r;
                    memcpy(&r, d + 2*sizeof(int) + record_pos*sizeof(Record), sizeof(Record));
					
					// find secondary record from second file, by hashing the index_key
					char* byte_string = hash_function(record.index_key);
					char* msb = malloc((open_files[sindexDesc2].depth + 1)*sizeof(char));

					for (int i = 0; i < open_files[sindexDesc2].depth; i++) {
						msb[i] = byte_string[i];
					}

					msb[open_files[sindexDesc2].depth] = '\0';

					char* tmp;
					int bucket = strtol(msb, &tmp, 2);
					free(msb);

					int hash_block_index = bucket / HASH_CAP;
					int hash_block_pos = bucket % HASH_CAP;

					BF_Block* block2;
					BF_Block_Init(&block2);

					CALL_BF(BF_GetBlock(open_files[sindexDesc2].fd, 0, block2));
					char* metadata = BF_Block_GetData(block2);

					size = HASH_ID_LEN*sizeof(char) + 1*sizeof(char) + 2*sizeof(int);
					
					int actual_hash_block_id;
					memcpy(&actual_hash_block_id, metadata + size + hash_block_index*sizeof(int), sizeof(int));

					CALL_BF(BF_UnpinBlock(block2));

					CALL_BF(BF_GetBlock(open_files[sindexDesc2].fd, actual_hash_block_id, block2));
					char* hash_data = BF_Block_GetData(block2);

					int data_block_id;
					memcpy(&data_block_id, hash_data + hash_block_pos*sizeof(int), sizeof(int));

					BF_Block* data_block2;
					BF_Block_Init(&data_block2);

					CALL_BF(BF_GetBlock(open_files[sindexDesc2].fd, data_block_id, data_block2));
					char* data2 = BF_Block_GetData(data_block2);

					int no_records2;
					memcpy(&no_records2, data2 + 1*sizeof(int), sizeof(int));

					SecondaryRecord record2;
					size = 2*sizeof(int);

					for (int i = 0; i < no_records2; i++) {

						memcpy(&record2, data2 + size + i * sizeof(SecondaryRecord), sizeof(SecondaryRecord));

						// find maching records from 1st and 2nd secondary indexes 
						if ( strcmp(record.index_key, record2.index_key) == 0 ) {
							joins++;

							BF_Block* bb;
							BF_Block_Init(&bb);

							int block_pos = record2.tupleId / BLOCK_CAP; // get in which data_block is the record
							int record_pos = record2.tupleId % BLOCK_CAP;    // get the position of record in the data_block
							
							int p = open_files[sindexDesc2].which_primary;

							CALL_BF(BF_GetBlock(open_files[p].fd , block_pos, bb));
							char* d = BF_Block_GetData(bb);
						
							// primary record of second hash file
							Record rr;
							memcpy(&rr, d + size + record_pos*sizeof(Record), sizeof(Record));
							
							// print join in one line
							if (strcmp(which_key, "city")==0){
								printf("%s | id = %d | name = %s | surname = %s | id = %d | name = %s | surname = %s |\n", record.index_key, r.id, r.name, r.surname, rr.id, rr.name, rr.surname);
							}
							else if (strcmp(which_key, "surname") == 0) {
								printf("%s | id = %d | name = %s | city = %s | id = %d | name = %s | city = %s |\n", record.index_key, r.id, r.name, r.city, rr.id, rr.name, rr.city);
							}
							else {
								fprintf(stderr, "not available index-key\n");
								return HT_ERROR;
							}
						
							CALL_BF(BF_UnpinBlock(bb));
						}
					}

					CALL_BF(BF_UnpinBlock(data_block2));
					CALL_BF(BF_UnpinBlock(block2));

					BF_Block_Destroy(&data_block2);
					BF_Block_Destroy(&block2);

					CALL_BF(BF_UnpinBlock(b));
					
					if (joins == 0) {
						printf("Inner Join is empty\n");
					}

					printf("\n");
                }
                CALL_BF(BF_UnpinBlock(data_block));
            }
            CALL_BF(BF_UnpinBlock(block));
        }
		
        free(hash_block_ids);

	    BF_Block_Destroy(&block);
	    BF_Block_Destroy(&data_block);
	}

    return HT_OK;
}
