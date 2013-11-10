import os
import web
import logging
import common
import sys

from catdb import mysql
from catdb import models

mydir = os.path.dirname(__file__)

from web.httpserver import StaticApp
def translate_path(self, path):
    return mydir + path
StaticApp.translate_path = translate_path


def run(database, user, host, port, password):
    def load_sql(handler):
        web.ctx.orm = mysql.connect(database=database,
                                    user=user, host=hostname,
                                    port=port, password=password)
        web.ctx.models = models
        models.database_proxy.initialize(web.ctx.orm)
        try:
            return handler()
        except web.HTTPError:
            web.ctx.orm.commit()
            raise
        except:
            web.ctx.orm.rollback()
            raise
        finally:
            web.ctx.orm.commit()
            # If the above alone doesn't work, uncomment
            # the following line:
            #web.ctx.orm.expunge_all()

    import walker
    walker.configure_templates(mydir)

    app = web.application(walker.urls)
    app.add_processor(load_sql)
    app.run()

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Copy a sample into a second database.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if args.password:
        password = common.get_database_password(args.user, args.hostname, args.port)
    else:
        password = mysql.DEFAULT_PASSWORD

    run(database=args.database,
        user=args.user, host=args.hostname,
        port=args.port, password=password)
