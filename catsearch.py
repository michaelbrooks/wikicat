"""
Given a search term (partial category name), finds all categories that match the search.
The output is a single image showing those categories as pixels.
"""

import logging
import os, sys
from datetime import datetime
from string import Template

import common
from catdb import models
from catdb.models import Category, DataSetVersion
from catdb import mysql
from catdb.mysql import DEFAULT_PASSWORD
from catdb import bfs
from dbpedia import resource
from output.bitmap import BitMap


def catsearch(search, output_file, db):
    models.database_proxy.initialize(db)

    total = Category.select().count()

    categories = Category.select(Category.id).where(Category.name % search).tuples()
    ids = [c[0] for c in categories]

    bitmap = BitMap(total)
    frontier_color = (102, 255, 0)
    bitmap.color_numbers(ids, frontier_color)
    bitmap.save(output_file)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Illustrate the results of a search by category name.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("search",
                        help="Part of a category name (case sensitive)")

    parser.add_argument("output",
                        help="Output image file")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if args.password:
        password = common.get_database_password(args.user, args.hostname, args.port)
    else:
        password = DEFAULT_PASSWORD

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.hostname,
                       port=args.port, password=password)

    #mysql.trap_warnings()

    if not db:
        exit(1)

    if args.yes:
        models.use_confirmations(False)

    with common.timer:
        catsearch(search=args.search,
                  output_file=args.output,
                  db=db)

    print 'Search complete (%fs)' % common.timer.elapsed()