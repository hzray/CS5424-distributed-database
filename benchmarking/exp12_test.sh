#!/usr/bin/bash
# Node 1 - Exp 1~2

WORKLOAD=$1
# SCRIPT_PATH="/home/stuproj/cs4224m/cs5424_cassandra/src/main.py"
SCRIPT_PATH="../src/main.py"

if [ "$WORKLOAD" == "A" ]
then
	XACT_FILE_DIR="../../xact_files_A/"
else
	XACT_FILE_DIR="../../xact_files_B/"
fi

# echo ${XACT_FILE_DIR} 
for cid in {0..39..39}
do
    echo "Excuting Client $cid.txt for Workload $WORKLOAD"
    python $SCRIPT_PATH $WORKLOAD $cid < $XACT_FILE_DIR$cid.txt
done
