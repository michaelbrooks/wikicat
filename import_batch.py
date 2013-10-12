"""
This script is meant to be executable.

Input arguments are a dbpedia version number
and database connection information for where
to store the imported data.
"""

from dbpedia.resource import DBpediaResource
from catdb import models, insert
import catdb.mysql as mysql
from catdb.mysql import DEFAULT_PASSWORD

from dbpedia import datasets
import common

import logging
import time


def import_dataset(dataset, version, language, limit=None):

    models.create_tables(drop_if_exists=False)

    resource = DBpediaResource(dataset=dataset, version=version, language=language)
    incoming = datasets.get_collection(resource=resource)

    versionInstance = models.dataset_version(version=resource.version, language=resource.language, date=resource.date)

    with incoming as data:

        before = time.time()
        imported = insert.insert_dataset(data=data, dataset=dataset, version_instance=versionInstance, limit=limit)
        after = time.time()

        if imported:
            print "Imported %d %s in %f seconds" % (imported, dataset, after - before)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import data from dbpedia into a database.")
    common.add_dataset_args(parser)
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("--limit",
                        required=False,
                        default=None,
                        type=int,
                        help="number of rows to insert, for debugging")

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

    # point all the models at this database
    models.database_proxy.initialize(db)

    if args.yes:
        models.use_confirmations(False)

    imported = len(args.langs) * len(args.versions) * len(args.datasets)
    print "Selected %d datasets for import" % imported

    for language in args.langs:
        for version in args.versions:
            for dataset in args.datasets:
                print "Importing %s v%s in %s" %(dataset, version, language)
                import_dataset(dataset=dataset, version=version, language=language, limit=args.limit)
