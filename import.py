"""
This script is meant to be executable.

Input arguments are a dbpedia version number
and database connection information for where
to store the imported data.
"""

import catdb.mysql as mysql
from catdb import bulk_insert
from dbpedia import datasets

import logging
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    import argparse
    import getpass

    parser = argparse.ArgumentParser("Import data files from dbpedia into a database.")

    parser.add_argument("version",
                        help="DBpedia version number")

    parser.add_argument("--lang", default="en",
                        required=False,
                        help="DBpedia language")

    parser.add_argument("--database", "-d",
                        help="database name")

    parser.add_argument("--host", "-h",
                        help="database hostname")

    parser.add_argument("--port", "-p",
                        required=False,
                        default=None,
                        help="database port number")

    parser.add_argument("--user", "-u",
                        help="database username")

    args = parser.parse_args()
    password = getpass.getpass()

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.host,
                       port=args.port, password=password)

    if not db:
        exit(1)

    with datasets.category_labels(version=args.version, language=args.language) as data:
        imported = bulk_insert.category_labels(data, db, limit=10)
        logging.info("Imported %d category_labels" , imported)

