#!/bin/sh

TPCDS_DIR=~/tpcds
SCRIPT_DIR=$(pwd)

cd $TPCDS_DIR/tools
for i in {01..30}
do
	ds="2018-08-$i"
	echo $ds
	
	if [ ! -d $TPCDS_DIR/data/$ds ]; then
		echo "generate data"	
		mkdir -p $TPCDS_DIR/data/$ds
		./dsdgen -scale 1 -dir $TPCDS_DIR/data/$ds
	fi

	if [ $i -eq 01 ]; then
	    echo "dimension table"
		hive -hiveconf LOCATION=$TPCDS_DIR/data/$ds -f $SCRIPT_DIR/fixed_table.sql
	fi

	echo "import data"
	hive -hiveconf LOCATION=$TPCDS_DIR/data/$ds -hiveconf DS=$ds -f $SCRIPT_DIR/partitioned_table.sql
done