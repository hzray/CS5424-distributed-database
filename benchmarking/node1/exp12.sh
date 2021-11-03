#!/usr/bin/bash
# Node 1 - Exp 1~2

WORKLOAD=$1
CONS_LVL=$2
SCRIPT_PATH="/home/stuproj/cs4224m/cs5424_cassandra/src/main.py"

if [ "$WORKLOAD" == "A" ]
then
	XACT_FILE_DIR="/temp/project_files/project_files_cassandra/xact_files_A/"
else
	XACT_FILE_DIR="/temp/project_files/project_files_cassandra/xact_files_B/"
fi

# echo ${XACT_FILE_DIR} 
for cid in {0..39..5}
do
    echo "Excuting Client $cid.txt for Workload $WORKLOAD"
    python3 $SCRIPT_PATH $WORKLOAD $cid  < $XACT_FILE_DIR$cid.txt $CONS_LVL > ${cid}_performance_${WORKLOAD}.txt &
done
