#!/usr/bin/bash
SCRIPT_PATH="/home/stuproj/cs4224o/cs5424-distributed-database-group-project/cassandra/src/main.py"
XACT_FILE_A_DIR="/temp/project_files/project_files_cassandra/xact_files_A"
XACT_FILE_B_DIR="/temp/project_files/project_files_cassandra/xact_files_B"
OUTPUT_FILE_DIR="/home/stuproj/cs4224o/cs5424-distributed-database-group-project/cassandra/project-files/output-files/"

for i in {1..20..5}
do
    python3 ${SCRIPT_PATH} QUORUM < ${XACT_FILE_A_DIR}zach.txt | sed "s/^/Z,$i,/" > ${OUTPUT_FILE_DIR}expZ-cli$i &
    python3 ${SCRIPT_PATH} QUORUM < ${XACT_FILE_B_DIR}zach.txt | sed "s/^/Z,$i,/" > ${OUTPUT_FILE_DIR}expZ-cli$i &
done
