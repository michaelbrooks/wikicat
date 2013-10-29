#!/bin/bash

if [ $# -lt 6 ]; then
    echo "Usage: bfs_pics.sh ROOT_CATEGORY OUTPUT DEPTH ORDER USERNAME FROM_DATABASE"
    exit 1
fi

ROOT_CATEGORY=$1
OUTPUT=$2
DEPTH=$3
ORDER=$4

HOSTNAME="127.0.0.1"
USERNAME=$5
FROM_DATABASE=$6

COMMAND="bfs_pics.py \
    $ROOT_CATEGORY \
    --depth $DEPTH \
    --order $ORDER \
    --output $OUTPUT \
    -H $HOSTNAME \
    -u $USERNAME \
    -d $FROM_DATABASE \
    --password --yes --verbose"

if [ "$6" == "profile" ]; then
    python pycachegrind.py $COMMAND
else
    time python $COMMAND
fi
