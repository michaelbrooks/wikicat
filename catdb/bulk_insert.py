"""
Functions for importing datasets.
"""

import mysql
from confirm import query_yes_no

def category_labels(dataset, db, limit=None):
    from models import CategoryLabel

    if CategoryLabel.table_exists():
        table_name = CategoryLabel._meta.db_table
        database = db.database

        confirm = query_yes_no("Replace existing table `%s` on database `%s`?" %(table_name, database),
                               default="no")
        if not confirm:
            return

        # drop the table first
        CategoryLabel.drop_table()

    # create the table
    CategoryLabel.create_table()

    imported = 0
    with db.transaction():
        for category, label in dataset:
            CategoryLabel.create(category=category, label=label)
            imported += 1

            if limit is not None and limit == imported:
                break

    db.commit()

    return imported

def _test():
    import nose.tools as nt

    db = mysql.connect(database="wikicat",
                       user="root", host="localhost")

    nt.ok_(db)

    # some example data
    dataset = [
        (u'Category:Futurama', u'Futurama'),
        (u'Category:World_War_II', u'World War II'),
        (u'Category:Programming_languages', u'Programming languages'),
        (u'Category:Professional_wrestling', u'Professional wrestling'),
        (u'Category:Algebra', u'Algebra'),
        (u'Category:Anime', u'Anime'),
        (u'Category:Abstract_algebra', u'Abstract algebra'),
        (u'Category:Mathematics', u'Mathematics'),
        (u'Category:Linear_algebra', u'Linear algebra'),
        (u'Category:Calculus', u'Calculus'),
        (u'Category:Monarchs', u'Monarchs'),
        (u'Category:British_monarchs', u'British monarchs'),
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
