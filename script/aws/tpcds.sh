#!/bin/sh

SCRIPT_DIR=$(pwd)

echo "dimension table"
hive-script --run-hive-script --args -f $SCRIPT_DIR/fixed_table.sql -d INPUT=s3://dwr-snt/tpc-ds/2018-08-01

for i in {01..30}
do
	ds="2018-08-$i"
	echo "import data $ds"
	hive-script --run-hive-script --args -f $SCRIPT_DIR/partitioned_table.sql -d INPUT=s3://dwr-snt/tpc-ds/$ds -d DS=$ds
done
