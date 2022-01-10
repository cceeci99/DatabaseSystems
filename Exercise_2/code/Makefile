ht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

sht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/hash_file.c ./src/sht_file.c -lbf -o ./build/runner -O2

run:
	./build/runner 

valgrind:
	valgrind --leak-check=full ./build/runner

time:
	time ./build/runner 

clean:
	rm -f ./build/runner  files/hash_files/*.db files/logs/*.txt