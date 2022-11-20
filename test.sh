#!/bin/bash

gcc psort_3035782231.c -o psort_3035782231 -pthread

for th in 1 2 3 5 9 13 16; do
    for n in 1000000 5000000 10000000 50000000 100000000; do
        echo "[th=$th, n=$n]"
        ./psort_3035782231 $n $th
        echo ""
    done
done