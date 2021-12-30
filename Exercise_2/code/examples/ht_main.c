#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>

#include "bf.h"
#include "ht.h"
#include "hash_file.h"

const char* names[] = {
	"Yannis",
	"Christofos",
	"Sofia",
	"Marianna",
	"Vagelis",
	"Maria",
	"Iosif",
	"Dionisis",
	"Konstantina",
	"Theofilos",
	"Giorgos",
	"Dimitris"
};

const char* surnames[] = {
	"Ioannidis",
	"Svingos",
	"Karvounari",
	"Rezkalla",
	"Nikolopoulos",
	"Berreta",
	"Koronis",
	"Gaitanis",
	"Oikonomou",
	"Mailis",
	"Michas",
	"Halatsis"
};

const char* cities[] = {
	"Athens",
	"San Francisco",
	"Los Angeles",
	"Amsterdam",
	"London",
	"New York",
	"Tokyo",
	"Hong Kong",
	"Munich",
	"Miami"
};

#define CALL_OR_DIE(call)         \
	{                             \
		HT_ErrorCode code = call; \
		if (code != HT_OK) {      \
		printf("Error\n");        \
		exit(code);               \
		}                         \
	}

int main(char argc, char* argv[]) {
	if (argc != 4) {
		fprintf(stderr, "usage : ./build/runner [filename] [no_records] [global_depth]");
		exit(EXIT_FAILURE);
	}

	char* filename = argv[1];
	int no_records = atoi(argv[2]);
	int global_depth = atoi(argv[3]);

	char* temp = "files/logs/result_";
	char* results = malloc((strlen(argv[2]) + strlen(argv[3]) + strlen(temp) + 6) * sizeof(char));
	results[0] = '\0';
	strcat(results, temp);
	strcat(results, argv[2]);
	temp = "_";
	strcat(results, temp);
	strcat(results, argv[3]);
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

	BF_Init(LRU);

	CALL_OR_DIE(HT_Init());

	// Create hash file
	printf("CREATE HASH FILE\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");
	CALL_OR_DIE(HT_CreateIndex(filename, global_depth));
	printf("\n");


	printf("\nOPEN HASH FILE\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");
	int indexDesc;
	CALL_OR_DIE(HT_OpenIndex(filename, &indexDesc)); 
	printf("\n");

	// // Insert entries
	Record record;
	srand(time(NULL));
	int r;
	printf("\nINSERT ENTRIES\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");
	for (int id = 0; id < no_records; ++id) {
		record.id = id;
		r = rand() % 12;
		memcpy(record.name, names[r], strlen(names[r]) + 1);
		r = rand() % 12;
		memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
		r = rand() % 10;
		memcpy(record.city, cities[r], strlen(cities[r]) + 1);

		printf("Inserting record with id = %d , name  = %s , surname = %s , city = %s\n", record.id, record.name, record.surname, record.city);
		CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
	}
	printf("\n");

	// Print entries
	printf("\nPRINT ENTRIES\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");
	int id = rand() % no_records;
	printf("- For id %d :\n", id);
	CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));	
	printf("\n- For all entries :\n");
	CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
	printf("\n");

	// // Hash statistics
	printf("\nPRINT HASH STATISTICS\n");
	for (int i = 0; i < 150; i++) {
		printf("-");
	}
	printf("\n");
	CALL_OR_DIE(HashStatistics(filename));

	// Close file
	printf("CLOSE HASH FILE\n");
	for (int i = 0; i < 50; i++) {
		printf("-");
	}
	CALL_OR_DIE(HT_CloseFile(indexDesc));
	printf("\n");

	BF_Close();

	free(results);
	close(fd);

	return 0;
}
