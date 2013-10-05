"""
This file can connect to a MySQL database.
"""

import peewee
import logging

from models import database_proxy

log = logging.getLogger('catdb.mysql')

DEFAULT_PORT = 3306

def connect(database, user, host, password='', port=DEFAULT_PORT):
    log.info("Connecting to '%s' on %s@%s:%d", database, user, host, port)
    db = peewee.MySQLDatabase(database,
                              user=user, host=host, passwd=password, port=port,
                              autocommit=False)
    # autocommit set to false for performance in bulk insert statements

    # point all the models at this database
    database_proxy.initialize(db)

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


