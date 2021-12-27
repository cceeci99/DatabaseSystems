#!/bin/bash

make clean > /dev/null

printf "Running ...\n\n"

for no_records in 10 100 500 1000;
do
    for depth in 1 2 3 4 5 6 7 8 9 10 11 12 13; # max depth for block size 512 is 13
    do
        make ht > /dev/null
        ./build/runner files/hash_files/data.db $no_records $depth
        rm ./build/runner files/hash_files/data.db > /dev/null
    done
done

cd ./files/logs

printf "###### TESTS ######\n"

printf "\n### FOR 10 RECORDS ###\n\n" 
for i in result_10_*.txt;
do
    if grep -q "Total number of records : 10" $i; then
        printf "PASSED : %s\n" $i
    else
        printf "FAILED : %s\n" $i
    fi
done

printf "\n### FOR 100 RECORDS ###\n\n" 
for i in result_100_*.txt;
do
    if grep -q "Total number of records : 100" $i; then
        printf "PASSED : %s\n" $i
    else
        printf "FAILED : %s\n" $i
    fi
done

printf "\n### FOR 500 RECORDS ###\n\n" 
for i in result_500_*.txt;
do
    if grep -q "Total number of records : 500" $i; then
        printf "PASSED : %s\n" $i
    else
        printf "FAILED : %s\n" $i
    fi
done

printf "\n### FOR 1000 RECORDS ###\n\n" 
for i in result_1000_*.txt;
do
    if grep -q "Total number of records : 1000" $i; then
        printf "PASSED : %s\n" $i
    else
        printf "FAILED : %s\n" $i
    fi
done

printf "\n"
cd ../../