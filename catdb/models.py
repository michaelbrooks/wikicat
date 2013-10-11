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
import sys

log = logging.getLogger('catdb.models')

ARTICLE_MAX_LENGTH = 200
CATEGORY_MAX_LENGTH = 200
LABEL_MAX_LENGTH = 200

database_proxy = Proxy()  # Create a proxy for our db.

INSERT_BATCH_SIZE = 10000

# use 0 for no cache
CACHE_LIMIT = 1000
CACHE_CUT_FACTOR = 0.5

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


class Cache(object):

    def __init__(self, name, relatedClass, modelClass):
        self.name = name
        self.modelClass = modelClass
        self.relatedClass = relatedClass

        self.cache = {}
        self.reductions = 0
        self.relatives_created = 0
        self.cache_hits = 0
        self.cache_misses = 0

        # a list of field objects relevant to this cache
        self.fields = []
        for fname, field in modelClass._meta.fields.iteritems():
            if isinstance(field, ForeignKeyField) and field.rel_model == self.relatedClass:
                self.fields.append((fname, field))

        self.start_batch()

    def reduce_cache(self):

        desired_cache_size = round(CACHE_LIMIT * CACHE_CUT_FACTOR)
        threshold = len(self.cache) - desired_cache_size

        for k in self.cache.keys():
            if self.cache[k][1] < threshold:
                del self.cache[k]
            else:
                self.cache[k][1] -= threshold

    def add_cache(self, key, value):
        if CACHE_LIMIT:
            self.cache[key] = [value, len(self.cache)]

        return value

    def get_cache(self, key):
        if CACHE_LIMIT:
            pair = self.cache.get(key)
            if pair:
                return pair[0]

        return None

    def find_model(self, instanceName):
        """Downloads or retrieves from cache"""

        related = self.get_cache(instanceName)

        if not related:
            try:
                related = self.relatedClass.get(self.relatedClass.name == instanceName)
                self.cache_misses += 1
            except DoesNotExist:
                related = self.relatedClass.create(name=instanceName)
                self.relatives_created += 1
        else:
            self.cache_hits += 1

        self.add_cache(related.name, related)

        return related

    def start_batch(self):

        # this list of new records in this batch
        self.records = []

        # a pre-cache - names of needed related models are placed here before being retrieved
        self.to_lookup = set()

    def fill_fields(self, record):
        """Attaches related models from this Cache to the record"""

        for fname, field in self.fields:
            if fname not in record:
                continue

            relatedName = record[fname]
            related = self.get_cache(relatedName)

            if not related:
                # save them for later batch lookup
                self.to_lookup.add(relatedName)
                self.records.append(record)
            else:
                record[fname] = related.id
                self.cache_hits += 1

    def process_batch(self):
        """
        Checks if any related models needed by the current batch are on the server.
        If not, creates them.
        Matches the models in the batch with their related models.
        :return:
        """

        if len(self.to_lookup) != 0:

            # get all the items that weren't already in the cache
            relatedModels = self.relatedClass.select()\
                .where(self.relatedClass.name << list(self.to_lookup))

            # cache them
            for r in relatedModels:
                self.add_cache(r.name, r)
                self.cache_misses += 1

            # now assign them all to the batched records
            for record in self.records:
                for fname, field in self.fields:
                    if fname not in record:
                        continue

                    relatedName = record[fname]
                    related = self.get_cache(relatedName)

                    if not related:
                        # ok fine we'll make you a new one
                        related = self.relatedClass.create(name=relatedName)
                        self.add_cache(related.name, related)
                        self.relatives_created += 1

                    record[fname] = related.id

        # now we shrink the cache if needed, since we're done with these for now
        if len(self.cache) >= CACHE_LIMIT:
            self.reduce_cache()
            self.reductions += 1


        self.start_batch()

    def print_stats(self):
        log.info("%s cache \t hits: %d; misses: %d; new relatives: %d",
                 self.name,
                 self.cache_hits, self.cache_misses, self.relatives_created)
        if self.cache_hits + self.cache_misses > 0:
            percentHits = 100 * self.cache_hits / (self.cache_hits + self.cache_misses)
        else:
            percentHits = 0

        log.info("        \t hits: %.1f%%; cache limit: %d; reductions: %d",
                 percentHits, CACHE_LIMIT, self.reductions)

def insert_dataset(data, dataset, version_instance, limit=None):
    if dataset not in model_mapping:
        raise Exception("No model for %s" % dataset)

    modelClass = model_mapping[dataset]

    # First thing we clear the instances associated with this version
    if hasattr(modelClass, 'version'):
        modelClass.delete().where(modelClass.version == version_instance).execute()

    db = modelClass._meta.database

    # for actually counting number imported
    imported = 0

    batch_counter = 0 # this is for controlling printout width
    batch = []

    # cache structures
    category_cache = Cache('categories', Category, modelClass)
    article_cache = Cache('articles', Article, modelClass)

    # disable autocommit and foreign key checks
    db.execute_sql('SET autocommit=0')
    db.execute_sql('SET foreign_key_checks=0')

    for record in data:

        article_cache.fill_fields(record)
        category_cache.fill_fields(record)

        # add the version reference to this record if needed
        if hasattr(modelClass, 'version'):
            record['version'] = version_instance.id

        # this record is now ready for insertion when the batch is full
        batch.append(record)

        # is the batch full?
        if INSERT_BATCH_SIZE is not None and len(batch) >= INSERT_BATCH_SIZE:
            article_cache.process_batch()
            category_cache.process_batch()

            # generate and run the sql and parameters for the batch insert
            sql, params = modelClass.generate_batch_insert(batch)
            db.execute_sql(sql, params)
            db.commit()

            imported += len(batch)
            batch_counter += 1

            sys.stdout.write('.')
            sys.stdout.flush()
            if batch_counter % 60 == 0:
                print

            # reset the caches
            batch = []

            if limit is not None and imported >= limit:
                print
                print "Reached limit of %d" % limit
                break

    print

    # just checking if we need to finish up
    if len(batch):
        article_cache.process_batch()
        category_cache.process_batch()

        sql, params = modelClass.generate_batch_insert(batch)
        db.execute_sql(sql, params)
        db.commit()
        imported += len(batch)

    article_cache.print_stats()
    category_cache.print_stats()

    db.execute_sql('SET autocommit=1')
    db.execute_sql('SET foreign_key_checks=1')

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
    imported = insert_dataset(data=dataset, dataset='category_labels', version_instance=datasetVersion)

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
