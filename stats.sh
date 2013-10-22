#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: stats.sh ITERATIONS USERNAME DATABASE"
    exit 1
fi

ITERATIONS=$1

HOSTNAME="127.0.0.1"
USERNAME=$2
DATABASE=$3

COMMAND="statistics.py \
    --iterations $ITERATIONS
    -H $HOSTNAME \
    -u $USERNAME \
    -d $DATABASE \
    --password --yes --verbose"

if [ "$4" == "profile" ]; then
    python pycachegrind.py $COMMAND
else
    python $COMMAND
fi
