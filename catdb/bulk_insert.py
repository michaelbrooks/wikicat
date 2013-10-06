"""
Functions for importing datasets.
"""

import mysql
from confirm import query_yes_no

import logging
log = logging.getLogger('catdb.bulk_insert')

INSERT_BATCH_SIZE = 10000

confirm_replacements = True
def use_confirmations(confirm):
    global confirm_replacements
    confirm_replacements = confirm

def insert_dataset(dataset, modelClass, db, limit=None):

    if modelClass.table_exists():
        table_name = modelClass._meta.db_table
        database = db.database

        if confirm_replacements:
            confirm = query_yes_no("Replace existing table `%s` on database `%s`?" %(table_name, database),
                                   default='no')
            if not confirm:
                return

        # drop the table first
        modelClass.drop_table()

    # create the table
    modelClass.create_table()

    imported = 0
    batch_idx = 0

    modelClass._meta.auto_increment = False
    header = "INSERT INTO category_labels (category, label) VALUES "

    batch = []
    for record in dataset:
        batch.append(record)

        if INSERT_BATCH_SIZE is not None and len(batch) >= INSERT_BATCH_SIZE:

            sql, params = modelClass.generate_batch_insert(batch)
            db.execute_sql(sql, params)
            db.commit()

            imported += len(batch)
            log.info("... inserted %d rows ...", imported)
            batch = []

        if limit is not None and imported >= limit:
            break

    if len(batch):
        sql, params = modelClass.generate_batch_insert(batch)
        db.execute_sql(sql, params)
        db.commit()
        imported += len(batch)

    return imported

def category_labels(dataset, db, limit=None):
    from models import CategoryLabel
    return insert_dataset(dataset, CategoryLabel, db, limit=limit)


def _test():
    import nose.tools as nt

    db = mysql.connect(database="wikicat",
                       user="root", host="localhost")

    nt.ok_(db)

    # some example data
    dataset = [
        {'category': u'Category:Futurama', 'label': u'Futurama'},
        {'category': u'Category:World_War_II', 'label': u'World War II'},
        {'category': u'Category:Programming_languages', 'label': u'Programming languages'},
        {'category': u'Category:Professional_wrestling', 'label': u'Professional wrestling'},
        {'category': u'Category:Algebra', 'label': u'Algebra'},
        {'category': u'Category:Anime', 'label': u'Anime'},
        {'category': u'Category:Abstract_algebra', 'label': u'Abstract algebra'},
        {'category': u'Category:Mathematics', 'label': u'Mathematics'},
        {'category': u'Category:Linear_algebra', 'label': u'Linear algebra'},
        {'category': u'Category:Calculus', 'label': u'Calculus'},
        {'category': u'Category:Monarchs', 'label': u'Monarchs'},
        {'category': u'Category:British_monarchs', 'label': u'British monarchs'},
    ]

    imported = category_labels(dataset, db)

    nt.assert_equal(len(dataset), imported)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)
