"""
This runnable script is meant to generate a second dataset
that is a smaller subset of the primary dataset.

A subset is defined by a root category.
All subcategories (to some fixed distance)
and their pages will be added to the subset.
"""

from catdb import models
from catdb import mysql
from catdb.mysql import DEFAULT_PASSWORD
from catdb.models import Category
from catdb import bfs

import logging
import common

def print_subset(root_category_name):

    root = Category.get(Category.name==root_category_name)
    descendants = bfs.descendants(root, norepeats=True)

    for cat in descendants:
        print cat.name


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import data from dbpedia into a database.")
    common.add_dataset_args(parser)
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("root_category",
                        help="Name of root category")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if args.password:
        password = common.get_database_password()
    else:
        password = DEFAULT_PASSWORD

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.hostname,
                       port=args.port, password=password)

    #mysql.trap_warnings()

    if not db:
        exit(1)

    # point all the models at this database
    models.database_proxy.initialize(db)

    if args.yes:
        models.use_confirmations(False)

    print_subset(args.root_category)