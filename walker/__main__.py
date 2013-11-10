import os
import web
from catdb import mysql
from catdb import models

mydir = os.path.dirname(__file__)

from web.httpserver import StaticApp
def translate_path(self, path):
    return mydir + path
StaticApp.translate_path = translate_path

def load_sql(handler):
    web.ctx.orm = mysql.connect(database="wikicat")
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


def run():
    import walker
    walker.configure_templates(mydir)

    app = web.application(walker.urls)
    app.add_processor(load_sql)
    app.run()

if __name__ == "__main__":
    run()
