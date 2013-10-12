"""
This file can be used to insert a set of
article_categories.
"""

__all__ = ['ArticleCategory', 'CategoryCategory', 'CategoryLabel',
           'database_proxy', 'use_confirmations', 'set_model_versions']

from peewee import ForeignKeyField, CharField, PrimaryKeyField, DateField
from peewee import Model, DoesNotExist
from playhouse.proxy import Proxy
from confirm import query_yes_no

import logging

log = logging.getLogger('catdb.models')

ARTICLE_MAX_LENGTH = 200
CATEGORY_MAX_LENGTH = 200
LABEL_MAX_LENGTH = 200

database_proxy = Proxy()  # Create a proxy for our db.

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
        columns = []
        for fname, field in cls._meta.fields.iteritems():
            if fname in example:
                columns.append(field.db_column)

        parts.append("(")
        parts.append(",".join('%s%s%s' % (quote_char, f, quote_char) for f in columns))
        parts.append(")")

        parts.append("VALUES")

        params = []

        settings = []
        for d in dictionaries:
            values = []

            for fname in cls._meta.fields:
                if fname in d:
                    values.append(d[fname])

            placeholder = ",".join(interpolation for v in values)
            settings.append("(%s)" % placeholder)
            params.extend(values)

        parts.append(",".join(settings))
        sql = " ".join(parts)

        return sql, params

class DataSetVersion(BaseModel):
    id = PrimaryKeyField()
    version = CharField(index=True, max_length=10)
    language = CharField(max_length=10)
    date = DateField()

    class Meta:
        db_table = 'dataset_versions'

# Articles may or may not exist in different versions
class Article(BaseModel):
    id = PrimaryKeyField()
    name = CharField(index=True, max_length=ARTICLE_MAX_LENGTH)

    class Meta:
        db_table = 'articles'

# Categories may or may not exist in different versions
class Category(BaseModel):
    id = PrimaryKeyField()
    name = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)

    def get_parents(self):
        return Category.select() \
            .join(CategoryCategory, on=CategoryCategory.broader) \
            .where(CategoryCategory.narrower == self)

    def get_children(self):
        return Category.select() \
            .join(CategoryCategory, on=CategoryCategory.narrower) \
            .where(CategoryCategory.broader == self)

    class Meta:
        db_table = 'categories'

# An abstract model that has a version
class VersionedModel(BaseModel):
    version = ForeignKeyField(DataSetVersion)

class CategoryLabel(VersionedModel):
    category = ForeignKeyField(Category, related_name="labels")
    label = CharField(max_length=CATEGORY_MAX_LENGTH, null=True)

    class Meta:
        db_table = 'category_labels'

class ArticleCategory(VersionedModel):
    article = ForeignKeyField(Article)
    category = ForeignKeyField(Category)

    class Meta:
        db_table = 'article_categories'

class CategoryCategory(VersionedModel):
    narrower = ForeignKeyField(Category, related_name="parents")
    broader = ForeignKeyField(Category, related_name="children")

    class Meta:
        db_table = 'category_categories'

model_mapping = {
    'article_categories': ArticleCategory,
    'category_categories': CategoryCategory,
    'category_labels': CategoryLabel
}

def create_tables(drop_if_exists=False):

    #foreign key dependencies
    modelClasses = [CategoryLabel, ArticleCategory, CategoryCategory, Article, Category, DataSetVersion]

    if drop_if_exists:
        for modelClass in modelClasses:

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

    #foreign key dependencies
    modelClasses.reverse()

    for modelClass in modelClasses:
        if not modelClass.table_exists():
            # create the table
            modelClass.create_table()

def dataset_version(version, language, date):
    # find or create an entry for this dataset version
    try:
        return DataSetVersion.get(version=version, language=language, date=date)
    except DoesNotExist:
        log.info("Created versions entry for %s %s", language, version)
        return DataSetVersion.create(version=version, language=language, date=date)

def _test():
    import nose.tools as nt
    import mysql
    import insert

    db = mysql.connect(database="wikicat",
                       user="root", host="localhost")

    nt.ok_(db)

    # point all the models at this database
    database_proxy.initialize(db)
    use_confirmations(False)

    # create the tables
    create_tables(drop_if_exists=True)

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

    datasetVersion = dataset_version(version='3.9', language='en', date='2013-04-03')
    imported = insert.insert_dataset(data=dataset, dataset='category_labels', version_instance=datasetVersion)

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
