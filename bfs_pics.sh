#!/bin/bash

if [ $# -lt 4 ]; then
    echo "Usage: bfs_pics.sh ROOT_CATEGORY DEPTH ORDER USERNAME FROM_DATABASE"
    exit 1
fi

ROOT_CATEGORY=$1
DEPTH=$2

HOSTNAME="127.0.0.1"
USERNAME=$3
FROM_DATABASE=$4

COMMAND="bfs_pics.py \
    $ROOT_CATEGORY
    --depth $DEPTH
    --order $ORDER
    -H $HOSTNAME \
    -u $USERNAME \
    -d $FROM_DATABASE \
    --password --yes --verbose"

if [ "$6" == "profile" ]; then
    python pycachegrind.py $COMMAND
else
    time python $COMMAND
fi
