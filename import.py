"""
This script is meant to be executable.

Input arguments are a dbpedia version number
and database connection information for where
to store the imported data.
"""

from catdb import models
import catdb.mysql as mysql
from catdb.mysql import DEFAULT_PORT, DEFAULT_HOST, DEFAULT_USER, DEFAULT_PASSWORD

from dbpedia import datasets, resource
from dbpedia import DEFAULT_LANGUAGE, DEFAULT_VERSION

import logging
import time


def import_dataset(dataset, version, language, limit=None):

    models.set_table_names(version=version, language=language)

    incoming = datasets.get_collection(dataset=dataset, version=version, language=language)

    with incoming as data:

        before = time.time()
        imported = models.insert_dataset(dataset, data, limit=limit)
        after = time.time()

        if imported:
            print "Imported %d category_labels in %f seconds" % (imported, after - before)

def add_database_args(parser):
    """
    Add arguments to the argparse parser for connecting to a database.
    :param parser:
    :return:
    """

    parser.add_argument("--database", "-d",
                        required=True,
                        help="database name")

    parser.add_argument("--hostname", "-H",
                        default=DEFAULT_HOST,
                        required=False,
                        help="database hostname")

    parser.add_argument("--port", "-P",
                        required=False,
                        default=DEFAULT_PORT,
                        help="database port number")

    parser.add_argument("--user", "-u",
                        required=False,
                        default=DEFAULT_USER,
                        help="database username")

    parser.add_argument("--password", "-p",
                        required=False,
                        default=False,
                        action="store_true",
                        help="use a password")

def add_dataset_args(parser):
    """
    Add arguments to the argparse parser for
    specifying a DBpedia dataset.
    :param parser:
    :return:
    """

    parser.add_argument("--datasets",
                        required=False,
                        nargs='+',
                        metavar='DBPEDIA_DATASET',
                        choices=resource.dataset_names,
                        default=[],
                        help="Which dataset(s) to import")

    parser.add_argument("--versions", "-v",
                        required=False,
                        metavar='DBPEDIA_VERSION',
                        nargs='+',
                        default=[DEFAULT_VERSION],
                        choices=resource.version_names,
                        help="Which DBpedia version number(s) to import")

    parser.add_argument("--langs", default=[DEFAULT_LANGUAGE],
                        required=False,
                        metavar="LANGUAGE_CODE",
                        nargs='+',
                        help="Which DBpedia language(s) to import")


def get_database_password():
    """
    Prompts the user to enter a password.
    :return:
    """
    return getpass.getpass(prompt="Enter db password for %s@%s:%s: " %(args.user, args.hostname, args.port))

if __name__ == "__main__":
    import argparse
    import getpass

    parser = argparse.ArgumentParser(description="Import data from dbpedia into a database.")
    add_dataset_args(parser)
    add_database_args(parser)

    parser.add_argument("--verbose",
                        required=False,
                        default=False,
                        action='store_true',
                        help="Print lots of messages")

    parser.add_argument("--yes",
                        required=False,
                        default=False,
                        action="store_true",
                        help="answer yes to all confirmations")

    parser.add_argument("--limit",
                        required=False,
                        default=None,
                        help="number of rows to insert, for debugging")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if args.password:
        password = get_database_password()
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
