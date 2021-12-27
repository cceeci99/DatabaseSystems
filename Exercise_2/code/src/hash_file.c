#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#include "sht_file.h"


#define BLOCK_CAP (int) ( (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record) )	// max records per block
#define HASH_CAP  (int) ( BF_BLOCK_SIZE / sizeof(int) )	                    	// max buckets per block

#define MAX_DEPTH 13
#define HASH_ID_LEN 9

// Assumptions:
// - each bucket maps to one block
// - record.id >= 0
// - max depth = 13 for block size 512 , because the max no_hash_block ids we can store in metadata block is (512 - 17) / 4 = 123 and for depth 14 we need 2 ^ 14 / HASH_CAP = 128 hash blocks
// - we consider that the ids are uniformly distributed

static char* hash_function(int id) {
	static int no_buckets = 2 << (MAX_DEPTH - 1);	

	// Hash the id
	id = ((id >> 16) ^ id) * 0x45d9f3b;
    id = ((id >> 16) ^ id) * 0x45d9f3b;
    id = (id >> 16) ^ id;
    id = id % no_buckets;
	
	// Convert the result to a binary string of no_digits chars
	static char b[MAX_DEPTH + 1];
    b[0] = '\0';
    int z;
    for (z = no_buckets / 2; z > 0 ; z >>= 1) {
        strcat(b, ((id & z) == z) ? "1" : "0");
    }

    return b;
}


#define CALL_BF(call)         \
{                             \
	BF_ErrorCode code = call; \
	if (code != BF_OK) {      \
		BF_PrintError(code);  \
		return HT_ERROR;      \
	}                         \
}

HT_ErrorCode HT_Init() {
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

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
	printf("Creating a hash file with name %s and initial global depth %d\n", filename, depth);

	CALL_BF(BF_CreateFile(filename)); 
	
	int fd;
	CALL_BF(BF_OpenFile(filename, &fd));

	// Allocate metadata block
	BF_Block* metadata_block;
	BF_Block_Init(&metadata_block);
	CALL_BF(BF_AllocateBlock(fd, metadata_block));

	size_t metadata_size = 0;

	// Initialize metadata block to 0
	char* metadata = BF_Block_GetData(metadata_block);
	memset(metadata + metadata_size, 0, BF_BLOCK_SIZE);

	// Update hash_id in metadata block
	char* hash_id = "HashFile";
	memcpy(metadata, hash_id, HASH_ID_LEN * sizeof(char));
	metadata_size += HASH_ID_LEN * sizeof(char);

	// Update depth in metadata block
	memcpy(metadata + metadata_size, &depth, sizeof(int));
	metadata_size += 1 * sizeof(int);

	// Calculate blocks assigned to hash table
	int no_buckets = 2 << (depth - 1);  // 2 ^ depth
	int no_hash_blocks = (no_buckets > HASH_CAP) > 0 ? no_buckets / HASH_CAP : 1;

	// Update no_hash_blocks in metadata block
	memcpy(metadata + metadata_size, &no_hash_blocks, sizeof(int));
	metadata_size += 1 * sizeof(int);

	BF_Block *hash_block, *data_block;
	BF_Block_Init(&hash_block);
	BF_Block_Init(&data_block);

	// If we need more than 1 hash block then all of them will be full, because the number of bucekts get doubled
	int limit = no_hash_blocks > 1 ? HASH_CAP : no_buckets;

	for (int i = 0; i < no_hash_blocks; i++) {

		// Allocate and initialize new hash block to 0
		CALL_BF(BF_AllocateBlock(fd, hash_block));
		char* hash_data = BF_Block_GetData(hash_block);
		memset(hash_data, 0, BF_BLOCK_SIZE);

		// Update metadata with the hash_block_id of each new hash block
		int no_blocks;
		CALL_BF(BF_GetBlockCounter(fd, &no_blocks));
		int hash_block_id = no_blocks - 1;
		memcpy(metadata + metadata_size + i * sizeof(int), &hash_block_id, sizeof(int));

		// Allocate 1 data block for each bucket - initialization phase there cannot be buddies,
		// we preallocate because it is easier this way to control buddies in the future.

		for (int j = 0; j < limit; j++) {

			// Allocate new data block and update its local depth and number of records
			CALL_BF(BF_AllocateBlock(fd, data_block));
			char* data = BF_Block_GetData(data_block);

			// Data block's structure :
			// - 0-3 bytes -> local depth
			// - 4-7 bytes -> number of records
			// - rest are records

			int local_depth = depth;
			memcpy(data, &local_depth, sizeof(int));

			int no_records;
			memcpy(data + 1 * sizeof(int), &no_records, sizeof(int));

			memset(data + 2 * sizeof(int), 0, BF_BLOCK_SIZE - 2 * sizeof(int));

			// We changed the data block -> set dirty & unpin
			BF_Block_SetDirty(data_block);
			CALL_BF(BF_UnpinBlock(data_block));

			// Get file's number of blocks
			int no_file_blocks;
			CALL_BF(BF_GetBlockCounter(fd, &no_file_blocks));

			// Update bucket's content with the new data block id
			int new_data_block_id = no_file_blocks - 1;
			memcpy(hash_data + j * sizeof(int), &new_data_block_id, sizeof(int));

		}
		
		// We changed the hash block -> set dirty & unpin
		BF_Block_SetDirty(hash_block);
		CALL_BF(BF_UnpinBlock(hash_block));
	}

	// We changed the metadata block -> set dirty and unpin
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

HT_ErrorCode HT_OpenIndex(const char *filename, int *indexDesc) {
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

	// Unpin metadata block - we just read from it
	CALL_BF(BF_UnpinBlock(block));
	
	// Update open_files with an entry of the recently opened hash file
	int flag = 1, i = 0;
	while (flag && i < MAX_OPEN_FILES) {
		if (open_files[i].fd == -1) {  // Find first available position
			printf("Attaching hash file with name '%s', fd=%d, depth=%d, no_hash_blocks=%d to open_files[%d]\n", filename, fd, depth, no_hash_blocks, i);
			open_files[i].fd = fd;
			open_files[i].depth = depth;
			open_files[i].inserted = 0;
			open_files[i].no_buckets = 2 << (depth - 1);
			open_files[i].no_hash_blocks = no_hash_blocks;
			open_files[i].filename = filename;
			*indexDesc = i;  // position in open_files array
			flag = 0;
		}
		i++;
	}
	
	// Free BF_block pointer
	BF_Block_Destroy(&block);

	// Reached limit of open files
	if (flag) {
		fprintf(stderr, "Error: maximum number of open hash files is %d\n", MAX_OPEN_FILES);
		return HT_ERROR;
	}

	return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
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

	return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record, int* tupleID, UpdateRecordArray *updateArray) {
	if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES) {
		fprintf(stderr, "Error: index out of bounds\n");
		return HT_ERROR;
	}

	// Get the hashed value of the id
	char* byte_string = hash_function(record.id);
	
	// Get depth MSB of hashed value
	char* msb = malloc((open_files[indexDesc].depth + 1) * sizeof(char));
	for (int i = 0; i < open_files[indexDesc].depth; i++) {
		msb[i] = byte_string[i];
	}
	msb[open_files[indexDesc].depth] = '\0';

	// Get the bucket based on the MSB
	char* tmp;
	int bucket = strtol(msb, &tmp, 2);
	free(msb);

	int hash_block_index = bucket / HASH_CAP;   // in which hash block is the bucket
	int hash_block_pos = bucket % HASH_CAP;		// which is the position of the bucket in that hash block

	BF_Block* block;
	BF_Block_Init(&block);

	CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
	char* metadata = BF_Block_GetData(block);

	int actual_hash_block_id;
	size_t sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int);
	memcpy(&actual_hash_block_id, metadata + sz + hash_block_index * sizeof(int), sizeof(int));
	
	int no_hash_blocks = open_files[indexDesc].no_hash_blocks;

	// Unpin metdata block
	CALL_BF(BF_UnpinBlock(block));

	// Fetch the specified hash block
	CALL_BF(BF_GetBlock(open_files[indexDesc].fd, actual_hash_block_id, block));
	char* hash_data = BF_Block_GetData(block);

	// Fetch the specified bucket
	int data_block_id;
	memcpy(&data_block_id, hash_data + hash_block_pos * sizeof(int), sizeof(int));

	// Unpin hash block
	CALL_BF(BF_UnpinBlock(block));

	BF_Block* data_block;
	BF_Block_Init(&data_block);

	CALL_BF(BF_GetBlock(open_files[indexDesc].fd, data_block_id, data_block));
	char* data = BF_Block_GetData(data_block);

	int local_depth;
	memcpy(&local_depth, data, sizeof(int));

	int no_records;
	memcpy(&no_records, data + 1 * sizeof(int), sizeof(int));

	int created_new_blocks = 0;

	// Case 2.1: the data block has enough space to store the given record
	if (no_records < BLOCK_CAP) {

		// Insert record in data block
		sz = 2 * sizeof(int) + no_records * sizeof(Record);
		memcpy(data + sz, &record, sizeof(Record));

		// Update data block's number of records
		no_records++;
		memcpy(data + 1 * sizeof(int), &no_records, sizeof(int));

		// We changed the data block -> set dirty & unpin
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
					sz = HASH_ID_LEN * sizeof(char) + 1 * sizeof(int);
					memcpy(&old_no_hash_blocks, metadata + sz, sizeof(int));

					sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int) + old_no_hash_blocks * sizeof(int);
					memcpy(metadata + sz, &new_hash_block_id, sizeof(int));

					int new_no_hash_blocks = old_no_hash_blocks + 1;
					sz = HASH_ID_LEN * sizeof(char) + 1 * sizeof(int);
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
				
				sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int);
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
			memcpy(metadata + HASH_ID_LEN * sizeof(char), &open_files[indexDesc].depth, sizeof(int));

			// We changed metadata block -> set dirty & unpin
			BF_Block_SetDirty(block);
			CALL_BF(BF_UnpinBlock(block));
		}

		// If we created new hash blocks we need to rehash the record.id to find in which hash block its data block id is stored
		if (created_new_blocks == 1) {

			// Get the hashed value of the id
			char* new_byte_string = hash_function(record.id);
			
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

			sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int);
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
		Record* records = malloc((no_records + 1) * sizeof(Record));
		for (int i = 0; i < no_records; i++) {
			sz = 2 * sizeof(int) + i * sizeof(Record);
			memcpy(&records[i], old_data_block_data + sz, sizeof(Record));
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
			if (HT_InsertEntry(indexDesc, records[i]) == HT_ERROR) {
				return HT_ERROR;
			}
			open_files[indexDesc].inserted--;  // avoid calculating same entry many times
		}
		free(records);
	}

	// One more successfull insertion :)
	open_files[indexDesc].inserted++;

	return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
	if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES) {
		fprintf(stderr, "Error: index out of bounds\n");
		return HT_ERROR;
	}

	if (id != NULL) {

		// Get the hashed value of the id
		char* byte_string = hash_function(*id);
		
		// Get depth MSB of hashed value
		char* msb = malloc((open_files[indexDesc].depth + 1) * sizeof(char));
		for (int i = 0; i < open_files[indexDesc].depth; i++) {
			msb[i] = byte_string[i];
		}
		msb[open_files[indexDesc].depth] = '\0';

		// Get the bucket based on the MSB
		char* tmp;
		int bucket = strtol(msb, &tmp, 2);
		free(msb);

		int hash_block_id = bucket / HASH_CAP;  // in which hash block is the bucket
		int hash_block_pos = bucket % HASH_CAP;		// which is the position of the bucket in that hash block

		BF_Block* block;
		BF_Block_Init(&block);

		CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
		char* metadata = BF_Block_GetData(block);
		
		int actual_hash_block_id;
		size_t sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int);
		memcpy(&actual_hash_block_id, metadata + sz + hash_block_id * sizeof(int), sizeof(int));

		// Unpin metadata block
		CALL_BF(BF_UnpinBlock(block));

		CALL_BF(BF_GetBlock(open_files[indexDesc].fd, actual_hash_block_id, block));
		char* hash_data = BF_Block_GetData(block);

		// Fetch the specified bucket
		int data_block_id;
		memcpy(&data_block_id, hash_data + hash_block_pos * sizeof(int), sizeof(int));

		BF_Block* data_block;
		BF_Block_Init(&data_block);

		CALL_BF(BF_GetBlock(open_files[indexDesc].fd, data_block_id, data_block));
		char* data = BF_Block_GetData(data_block);

		int no_records;
		memcpy(&no_records, data + 1 * sizeof(int), sizeof(int));

		Record record;
		int counter = 0;
		sz = 2 * sizeof(int);
		for (int i = 0; i < no_records; i++) {
			memcpy(&record, data + sz + i * sizeof(Record), sizeof(Record));
			if (record.id == *id) {
				printf("id = %d , name = %s , surname = %s , city = %s \n", record.id, record.name, record.surname, record.city);
				counter++;
			}
		}

		if (counter == 0) {
			printf("No records found with id %d\n", *id);
		}

		CALL_BF(BF_UnpinBlock(block));
		CALL_BF(BF_UnpinBlock(data_block));

		BF_Block_Destroy(&block);
		BF_Block_Destroy(&data_block);
	}
	else {
		BF_Block* block;
		BF_Block_Init(&block);

		CALL_BF(BF_GetBlock(open_files[indexDesc].fd, 0, block));
		char* metadata = BF_Block_GetData(block);

		int no_hash_blocks = open_files[indexDesc].no_hash_blocks;

		int* hash_block_ids = malloc(no_hash_blocks * sizeof(int));

		size_t sz = HASH_ID_LEN * sizeof(char) + 2 * sizeof(int);
		for (int i = 0; i < no_hash_blocks; i++) {
			memcpy(&hash_block_ids[i], metadata + sz + i * sizeof(int), sizeof(int));
		}

		// Unpin metadata block
		CALL_BF(BF_UnpinBlock(block));

		BF_Block* data_block;
		BF_Block_Init(&data_block);

		int current_id = 0, counter = 0;

		for (int i = 0; i < no_hash_blocks; i++) {
			CALL_BF(BF_GetBlock(open_files[indexDesc].fd, hash_block_ids[i], block));
			char* hash_data = BF_Block_GetData(block);

			int limit = no_hash_blocks == 1 ? open_files[indexDesc].no_buckets : HASH_CAP;

			for (int j = 0; j < limit; j++) {
				int data_block_id;
				memcpy(&data_block_id, hash_data + j * sizeof(int), sizeof(int));

				// To avoid printing same records because of buddies
				if (current_id != data_block_id) {
					current_id = data_block_id;
				}
				else {
					continue;
				}

				CALL_BF(BF_GetBlock(open_files[indexDesc].fd, data_block_id, data_block));
				char* data = BF_Block_GetData(data_block);

				int no_records;
				memcpy(&no_records, data + 1 * sizeof(int), sizeof(int));

				counter += no_records;

				Record record;
				sz = 2 * sizeof(int);
				for (int k = 0; k < no_records; k++) {
					memcpy(&record, data + sz + k * sizeof(Record), sizeof(Record));
					printf("id = %d , name = %s , surname = %s , city = %s \n", record.id, record.name, record.surname, record.city);
				}

				// Unpin data block
				CALL_BF(BF_UnpinBlock(data_block));
			}

			// Unpin hash block
			CALL_BF(BF_UnpinBlock(block));
		}

		free(hash_block_ids);

		printf("\nTotal number of records : %d\n", counter);

		BF_Block_Destroy(&block);
		BF_Block_Destroy(&data_block);
	}

	return HT_OK;
}


HT_ErrorCode HashStatistics(char* filename) {
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

		int min_no_records = BLOCK_CAP;
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