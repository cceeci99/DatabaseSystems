ht:
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

bf:
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

sht:
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c -lbf -o ./build/runner -02

all:
	./run.sh

run:
	./build/runner files/hash_files/data.db 1000 2

time:
	time ./build/runner files/hash_files/data.db 1000 2

clean:
	rm -f ./build/runner files/hash_files/*.db files/logs/*.txt