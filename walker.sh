#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: walker.sh USERNAME DATABASE"
    exit 1
fi

HOSTNAME="127.0.0.1"
USERNAME=$2
DATABASE=$3

COMMAND="walker 8080 \
    -H $HOSTNAME \
    -u $USERNAME \
    -d $DATABASE \
    --password --yes --verbose"

python -m $COMMAND