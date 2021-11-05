#!/usr/bin/bash

WORKLOAD=$1
SERVER_NUM=$2


SCRIPT_PATH_A="/home/stuproj/cs4224m/cs5424_cockroach/scripts/main-singleA.py"
SCRIPT_PATH_B="/home/stuproj/cs4224m/cs5424_cockroach/scripts/main-singleB.py"
REPORT_PATH="/home/stuproj/cs4224m/cs5424_cockroach/output/"
PROJECT_FILES="/home/stuproj/cs4224m/project_files/"
VENV_PATH="/home/stuproj/cs4224m/cs5424_cockroach/venv/"

#SCRIPT_PATH="/Users/Administrator/PycharmProjects/CS5424-neliy/cockroach/main-singleA.py"
#REPORT_PATH="/Users/Administrator/PycharmProjects/CS5424-neliy/cockroach/output/"
#PROJECT_FILES="/Users/Administrator/Desktop/cs5424db/project_files/"

A_DIR="xact_files_A/"
B_DIR="xact_files_B/"
TEST_DI="my_xact/"


if [ "$WORKLOAD" == "A" ]
then
  XACT_FILE_PATH="$PROJECT_FILES$A_DIR"
  SCRIPT_PATH="$SCRIPT_PATH_A"
elif [ "$WORKLOAD" == "B" ]
then
  XACT_FILE_PATH="$PROJECT_FILES$B_DIR"
  SCRIPT_PATH="$SCRIPT_PATH_B"
else
  XACT_FILE_PATH="$PROJECT_FILES$TEST_DI"
fi

#ADDRESS="localhost"
#if [ "1" == "$SERVER_NUM" ]
#then
#  ADDRESS="192.168.51.14"
#elif [ "2" == "$SERVER_NUM" ]
#then
#  ADDRESS="192.168.51.15"
#elif [ "3" == "$SERVER_NUM" ]
#then
#  ADDRESS="192.168.51.16"
#elif [ "4" == "$SERVER_NUM" ]
#then
#  ADDRESS="192.168.51.17"
#else
#  ADDRESS="192.168.51.13"
#fi

if [ "1" == "$SERVER_NUM" ]
then
  ADDRESS="192.168.48.175"
elif [ "2" == "$SERVER_NUM" ]
then
  ADDRESS="192.168.48.176"
elif [ "3" == "$SERVER_NUM" ]
then
  ADDRESS="192.168.48.177"
elif [ "4" == "$SERVER_NUM" ]
then
  ADDRESS="192.168.48.178"
else
  ADDRESS="192.168.48.174"
fi

PORT="26277"
#if ["SERVER_NUM" == "0"];
#then
#  PORT="26257"
#elif ["SERVER_NUM" == "1"];
#then
#  PORT="26258"
#elif ["SERVER_NUM" == "2"];
#then
#  PORT="26259"
#elif ["SERVER_NUM" == "3"];
#then
#  PORT="26260"
#elif ["SERVER_NUM" == "4"];
#then
#  PORT="26261"
#fi

echo ${XACT_FILE_PATH}

for c in {0..39}
do
    if [ $(($c % 5)) -eq $SERVER_NUM ]
    then
#       python main-singleA.py -address='' -port='' -cid='' -xfpath='' -rfpath=''
        source "$VENV_PATH/bin/activate"
        python $SCRIPT_PATH "-address=$ADDRESS" "-port=$PORT" "-cid=$c" "-xfpath=$XACT_FILE_PATH" "-rfpath=$REPORT_PATH" &
    fi
done
