"""
Collection of utility functions common to several
runnable scripts.
"""

from catdb.mysql import DEFAULT_PORT, DEFAULT_HOST, DEFAULT_USER
from dbpedia import DEFAULT_LANGUAGE, DEFAULT_VERSION
from dbpedia import resource

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
    import getpass

    return getpass.getpass(prompt="Enter db password for %s@%s:%s: " %(args.user, args.hostname, args.port))


def add_io_args(parser):
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
