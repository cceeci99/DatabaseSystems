#ifndef HT_H
#define HT_H

#define MAX_OPEN_FILES 20


typedef enum HT_ErrorCode {
  HT_OK,
  HT_ERROR
} HT_ErrorCode;

typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
} Record;

typedef struct HF_Info {
    int fd;
    int depth;
	int inserted;
	int no_buckets;
	int no_hash_blocks;
	const char* filename;
} HF_Info;

// Global array
HF_Info open_files[MAX_OPEN_FILES];

#endif // HT_H