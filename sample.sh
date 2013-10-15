#!/bin/bash

if [ $# -lt 5 ]; then
    echo "Usage: sample.sh ROOT_CATEGORY DEPTH USERNAME FROM_DATABASE TARGET_DATABASE"
    exit 1
fi

ROOT_CATEGORY=$1
DEPTH=$2

HOSTNAME="127.0.0.1"
USERNAME=$3
FROM_DATABASE=$4
TARGET_DATABASE=$5

if [ "$TARGET_DATABASE" == "$FROM_DATABASE" ]; then
    echo "Source and target database should not be the same."
    exit 1
fi

COMMAND="subset.py \
    $ROOT_CATEGORY
    --depth $DEPTH
    -H $HOSTNAME \
    -u $USERNAME \
    -d $FROM_DATABASE \
    --target $TARGET_DATABASE
    --password --yes --verbose"

if [ "$6" == "profile" ]; then
    python pycachegrind.py $COMMAND
else
    python $COMMAND
fi
