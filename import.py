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

if __name__ == "__main__":
    import argparse
    import getpass

    parser = argparse.ArgumentParser("Import data files from dbpedia into a database.")

    parser.add_argument("dataset",
                        choices=resource.dataset_names,
                        help="Which dataset to import")

    parser.add_argument("--verbose",
                        required=False,
                        default=False,
                        action='store_true',
                        help="Print lots of messages")

    parser.add_argument("--version", "-v",
                        required=False,
                        default=DEFAULT_VERSION,
                        help="DBpedia version number")

    parser.add_argument("--lang", default=DEFAULT_LANGUAGE,
                        required=False,
                        help="DBpedia language")

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
        password = getpass.getpass(prompt="Enter db password for %s@%s:%s: " %(args.user, args.hostname, args.port))
    else:
        password = DEFAULT_PASSWORD

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.hostname,
                       port=args.port, password=password)

    if not db:
        exit(1)

    # point all the models at this database
    models.database_proxy.initialize(db)

    if args.yes:
        models.use_confirmations(False)

    models.set_model_versions(args.version)

    incoming = datasets.get_collection(dataset=args.dataset, version=args.version, language=args.lang)

    with incoming as data:

        before = time.time()
        imported = models.insert_dataset(args.dataset, data, limit=args.limit)
        after = time.time()

        if imported:
            print "Imported %d category_labels in %f seconds" % (imported, after - before)


