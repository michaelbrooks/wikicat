"""
This file can connect to a MySQL database.
"""

__all__ = ['connect', 'trap_warnings']

import peewee
import logging

log = logging.getLogger('catdb.mysql')

DEFAULT_USER = 'root'
DEFAULT_HOST = 'localhost'
DEFAULT_PASSWORD = ''
DEFAULT_PORT = 3306

def connect(database, user=DEFAULT_USER, host=DEFAULT_HOST, password=DEFAULT_PASSWORD, port=DEFAULT_PORT):
    log.info("Connecting to '%s' on %s@%s:%d", database, user, host, port)
    db = peewee.MySQLDatabase(database,
                              user=user, host=host, passwd=password, port=port,
                              autocommit=False)
    # autocommit set to false for performance in bulk insert statements

    try:
        db.connect()
    except Exception as e:
        log.error("Could not connect to mysql: %s", str(e))
        return False

    return db

def trap_warnings():
    """
    Turn MySQL warnings into errors.
    :return:
    """
    import warnings, MySQLdb
    warnings.filterwarnings('error', category=MySQLdb.Warning)


def _test():
    import nose.tools as nt

    db = connect('wikicat', user='root', host='localhost', password='')

    nt.ok_(db)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)


