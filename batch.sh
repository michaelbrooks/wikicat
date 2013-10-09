#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: import.sh USERNAME DATABASE"
    exit 1
fi

DATASETS="article_categories category_categories category_labels"
VERSIONS="2.0 3.0 3.9"
LANGUAGES="en"

HOSTNAME="127.0.0.1"
USERNAME=$1
DATABASE=$2

python import.py \
    --dataset $DATASETS \
    --version $VERSIONS \
    --lang $LANGUAGES \
    -H $HOSTNAME \
    -u $USERNAME \
    -d $DATABASE \
    --password --yes
