"""
Export a CSV describing the categories in the subtree
of a particular root, up to some depth.
"""
import os, sys
import logging
import csv

import common
from catdb import models
from catdb import mysql
from catdb import bfs
from dbpedia import resource
from catdb.mysql import DEFAULT_PASSWORD
from catdb.models import Category, DataSetVersion

def subtree(root_name, depth, output_filename, db, version_list=[]):
    models.database_proxy.initialize(db)

    root = Category.select().where(Category.name==root_name).first()

    versions = DataSetVersion.select()
    if len(version_list):
        versions = versions.where(DataSetVersion.version << version_list)

    with open(output_filename, 'wb') as outfile:
        fieldNames = ['version_id', 'version_version', 'version_date', 'depth', 'category_id', 'category_name']
        writer = csv.DictWriter(outfile, fieldNames)
        writer.writeheader()

        for version in versions:
            print "Getting subtree for version %s..." % version.version
            sys.stdout.flush()

            # get the bfs iterator
            descendants = bfs.descendants(root, norepeats=True, max_levels=depth, version=version)

            for cat in descendants:
                depth = descendants.current_level

                writer.writerow({
                    'version_id': str(version.id),
                    'version_version': version.version,
                    'version_date': version.date,
                    'depth': str(depth),
                    'category_id': str(cat.id),
                    'category_name': cat.name.encode('utf-8')
                })

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export a subtree.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("root_category",
                        help="Name of root category")

    parser.add_argument("--output",
                        default=None,
                        required=False,
                        help="Output csv file for subtree")

    parser.add_argument("--versions", "-v",
                        required=False,
                        metavar='DBPEDIA_VERSION',
                        nargs='*',
                        default=[],
                        choices=resource.version_names,
                        help="Which DBpedia version number(s) to search")

    parser.add_argument("--depth",
                        default=5,
                        type=int,
                        required=False,
                        help="Category depth to explore from root category")

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

    if args.output is None:
        output = common.slugify(unicode(args.root_category)) + ".csv"
        print "Saving to %s" % output
    else:
        output = args.output

    with common.timer:
        subtree(root_name=args.root_category,
                 depth=args.depth,
                 output_filename=output,
                 db=db,
                 version_list=args.versions)

    print 'Exported complete (%fs)' % common.timer.elapsed()
