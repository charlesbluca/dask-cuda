#!/bin/bash

for size in 200_000_000 400_000_000 800_000_000 1_600_000_000
do
    for cols in 100 200 300 400 500 600 700 800 900 1000
    do
        python pack_df.py $size $cols
    done
done