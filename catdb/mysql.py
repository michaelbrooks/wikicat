"""
This file can connect to a MySQL database.
"""

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



def _test():
    import nose.tools as nt

    db = connect('mysql', user='root', host='localhost', password='')

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


