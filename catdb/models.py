"""
This file can be used to insert a set of
article_categories.
"""

__all__ = ['ArticleCategory', 'CategoryCategory', 'CategoryLabel',
           'database_proxy', 'use_confirmations', 'set_model_versions']

from peewee import IntegerField, CharField, PrimaryKeyField
from peewee import Model
from playhouse.proxy import Proxy
from confirm import query_yes_no

import logging
import sys

log = logging.getLogger('catdb.models')

ARTICLE_MAX_LENGTH = 200
CATEGORY_MAX_LENGTH = 200
LABEL_MAX_LENGTH = 200

database_proxy = Proxy()  # Create a proxy for our db.

INSERT_BATCH_SIZE = 10000

confirm_replacements = True
def use_confirmations(confirm):
    global confirm_replacements
    confirm_replacements = confirm

class BaseModel(Model):
    class Meta:
        database = database_proxy  # Use proxy for our DB.

    @classmethod
    def generate_batch_insert(cls, dictionaries):
        """
        Generates a bulk insert statement a list of dictionaries
        representing model data.
        :param dictionaries:
        :return:
        """

        if len(dictionaries) == 0:
            return None

        # get an example dictionary
        example = dictionaries[0]

        quote_char = cls._meta.database.quote_char
        interpolation = cls._meta.database.interpolation

        parts = ['INSERT INTO %s%s%s' % (quote_char, cls._meta.db_table, quote_char)]
        fields = [f for f in cls._meta.fields if f in example]

        parts.append("(")
        parts.append(",".join('%s%s%s' % (quote_char, f, quote_char) for f in fields))
        parts.append(")")

        parts.append("VALUES")

        params = []

        settings = []
        for d in dictionaries:

            values = [d[f] for f in fields]
            placeholder = ",".join(interpolation for v in values)
            settings.append("(%s)" % placeholder)
            params.extend(values)

        parts.append(",".join(settings))
        sql = " ".join(parts)

        return sql, params

class ArticleCategory(BaseModel):
    id = PrimaryKeyField()
    article = CharField(index=True, max_length=ARTICLE_MAX_LENGTH)
    category = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)

    class Meta:
        db_table='article_categories'

class CategoryCategory(BaseModel):
    id = PrimaryKeyField()
    narrower = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)
    broader = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)

    class Meta:
        db_table='category_categories'

class CategoryLabel(BaseModel):
    id = PrimaryKeyField()
    category = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)
    label = CharField(index=True, max_length=LABEL_MAX_LENGTH)

    class Meta:
        db_table='category_labels'

model_mapping = {
    'article_categories': ArticleCategory,
    'category_categories': CategoryCategory,
    'category_labels': CategoryLabel
}

def set_table_names(version, language):
    """
    Customize the names of the database tables for this version and language.
    :param version:
    :param language:
    :return:
    """
    version_code = version.replace(".", "_")
    for m in model_mapping.values():
        m._meta.db_table += "_%s" % version_code

def insert_dataset(dataset, records, limit=None):
    if not dataset in model_mapping:
        raise Exception("No model for %s" % dataset)

    modelClass = model_mapping[dataset]
    db = modelClass._meta.database

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

    modelClass._meta.auto_increment = False

    batch_counter = 0
    batch = []
    for record in records:
        batch.append(record)

        if INSERT_BATCH_SIZE is not None and len(batch) >= INSERT_BATCH_SIZE:

            sql, params = modelClass.generate_batch_insert(batch)
            db.execute_sql(sql, params)
            db.commit()

            imported += len(batch)
            batch_counter += 1

            sys.stdout.write('.')
            if batch_counter % 60 == 0:
                print

            log.info("... inserted %d rows ...", imported)
            batch = []

        if limit is not None and imported >= limit:
            break

    print

    if len(batch):
        sql, params = modelClass.generate_batch_insert(batch)
        db.execute_sql(sql, params)
        db.commit()
        imported += len(batch)

    return imported


def _test():
    import nose.tools as nt
    import mysql

    db = mysql.connect(database="wikicat",
                       user="root", host="localhost")

    nt.ok_(db)

    # point all the models at this database
    database_proxy.initialize(db)
    use_confirmations(False)

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

    imported = insert_dataset('category_labels', dataset)

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
