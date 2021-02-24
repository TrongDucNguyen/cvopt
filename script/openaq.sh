#!/bin/sh

DATA_DIR=/home/trong/openaq/data
SCRIPT_DIR=$(pwd)

hive -f $SCRIPT_DIR/openaq.sql

for yyyy in {2015..2018}
do
    for mm in {01..12}
    do
        for dd in {01..31}
        do
            ds="$yyyy-$mm-$dd"
            if [ ! -f $DATA_DIR/$yyyy/$ds.csv ]; then
                echo "$ds      data is not available. Skip!"
            else
                echo "$ds      import data"
                hive -e "LOAD DATA LOCAL INPATH '$DATA_DIR/$yyyy/$ds.csv' OVERWRITE INTO TABLE openaq PARTITION(ds = '$ds')"
            fi
        done
    done
done