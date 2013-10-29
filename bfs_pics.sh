#!/bin/bash

if [ $# -lt 5 ]; then
    echo "Usage: bfs_pics.sh ROOT_CATEGORY OUTPUT DEPTH ORDER USERNAME FROM_DATABASE"
    exit 1
fi

ROOT_CATEGORY=$1
OUTPUT=$2
DEPTH=$3
VERSIONS="3.7 3.8 3.9"

HOSTNAME="127.0.0.1"
USERNAME=$4
FROM_DATABASE=$5

COMMAND="bfs_pics.py \
    $ROOT_CATEGORY \
    --depth $DEPTH \
    --output $OUTPUT \
    --versions $VERSIONS \
    -H $HOSTNAME \
    -u $USERNAME \
    -d $FROM_DATABASE \
    --password --yes --verbose"

if [ "$6" == "profile" ]; then
    python pycachegrind.py $COMMAND
else
    time python $COMMAND
fi
