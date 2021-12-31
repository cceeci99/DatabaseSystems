ht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

sht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/hash_file.c ./src/sht_file.c -lbf -o ./build/runner -O2

all:
	./run.sh

run:
	./build/runner files/hash_files/data.db 1000 2

debug:
	gdb ./build/runner files/hash_files/data.db 1000 2

time:
	time ./build/runner files/hash_files/data.db 1000 2

clean:
	rm -f ./build/runner files/hash_files/*.db files/logs/*.txt *.db *.txt