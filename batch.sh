#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: import.sh USERNAME DATABASE"
    exit 1
fi

DATASETS="article_categories category_categories category_labels"
VERSIONS="2.0 3.0 3.1 3.2 3.3 3.4 3.5 3.6 3.7 3.8 3.9"
LANGUAGES="en"

HOSTNAME="127.0.0.1"
USERNAME=$1
DATABASE=$2

COMMAND="import_batch.py \
    --dataset $DATASETS \
    --version $VERSIONS \
    --lang $LANGUAGES \
    -H $HOSTNAME \
    -u $USERNAME \
    -d $DATABASE \
    --password --yes --verbose"

if [ "$3" == "profile" ]; then
    python pycachegrind.py $COMMAND
else
    python $COMMAND
fi
