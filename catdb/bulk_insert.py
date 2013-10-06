"""
Functions for importing datasets.
"""

import mysql
from confirm import query_yes_no

def insert_dataset(dataset, modelClass, db):

    if modelClass.table_exists():
        table_name = modelClass._meta.db_table
        database = db.database

        confirm = query_yes_no("Replace existing table `%s` on database `%s`?" %(table_name, database),
                               default="no")
        if not confirm:
            return

        # drop the table first
        modelClass.drop_table()

    # create the table
    modelClass.create_table()

    imported = 0
    with db.transaction():
        for record in dataset:
            modelClass.create(**record)
            imported += 1

    db.commit()

    return imported

def category_labels(dataset, db):
    from models import CategoryLabel
    return insert_dataset(dataset, CategoryLabel, db)


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
