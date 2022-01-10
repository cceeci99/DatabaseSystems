#ifndef HT_H
#define HT_H

#define MAX_OPEN_FILES 20

#define MAX_DEPTH 13
#define HASH_ID_LEN 9

#define HASH_CAP  (int) (BF_BLOCK_SIZE / sizeof(int))	// max buckets per block same for primary and secondary index

typedef enum HT_ErrorCode {
  HT_OK,
  HT_ERROR
} HT_ErrorCode;


// Record is used by both SHT and HT
typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
} Record;


// UpdateRecordArray is used by both SHT and HT
typedef struct {
	char surname[20];
	char city[20];
	int oldTupleId; // old position of record before inserting the new record
	int newTupleId; // new position of record after it was moved by inserting the new record
	
} UpdateRecordArray;


typedef struct HF_Info {
    int fd;
    int depth;
	int inserted;
	int no_buckets;
	int no_hash_blocks;
	const char* filename;

	int index_type; 					// 1 for primary, 0 for secondary
	int which_primary;					// corresponding primary file of the secondary index that is created
	char which_index_key;				// 'c' for city, 's' for surname
	int split;							// 1 if there was a split on primary index 0, if not

} HF_Info;

// Global array
HF_Info open_files[MAX_OPEN_FILES];

#endif // HT_H