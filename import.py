"""
This script is meant to be executable.

Input arguments are a dbpedia version number
and database connection information for where
to store the imported data.
"""

from catdb import bulk_insert
import catdb.mysql as mysql
from catdb.mysql import DEFAULT_PORT, DEFAULT_HOST, DEFAULT_USER

from dbpedia import datasets
from dbpedia import DEFAULT_LANGUAGE, DEFAULT_VERSION

import logging

if __name__ == "__main__":
    import argparse
    import getpass

    parser = argparse.ArgumentParser("Import data files from dbpedia into a database.")

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

    parser.add_argument("--port", "-p",
                        required=False,
                        default=DEFAULT_PORT,
                        help="database port number")

    parser.add_argument("--user", "-u",
                        required=False,
                        default=DEFAULT_USER,
                        help="database username")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    password = getpass.getpass(prompt="Enter db password for %s@%s:%s: " %(args.user, args.hostname, args.port))

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.hostname,
                       port=args.port, password=password)

    if not db:
        exit(1)

    with datasets.category_labels(version=args.version, language=args.lang) as data:
        imported = bulk_insert.category_labels(data, db)
        print "Imported %d category_labels" & imported

