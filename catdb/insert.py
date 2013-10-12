"""
This file uses the model classes to import
a large batch of data.
"""

import sys

from models import Category, Article, model_mapping
from peewee import ForeignKeyField

INSERT_BATCH_SIZE = 10000

# use 0 for no cache
CACHE_LIMIT = 2000
CACHE_CUT_FACTOR = 0.5

import logging
log = logging.getLogger('catdb.insert')

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
                record[fname] = related['id']
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
            relatedModels = self.relatedClass.select() \
                .where(self.relatedClass.name << list(self.to_lookup)) \
                .dicts()

            # cache them
            for relatedDict in relatedModels:
                name = relatedDict['name']

                self.add_cache(name, relatedDict)
                self.to_lookup.remove(name)

                self.cache_misses += 1

            # now, do we need to create any?
            if len(self.to_lookup) > 0:
                # make a list out of the map for reliability
                to_lookup = list(self.to_lookup)

                newRelated = [{
                                  'name': name
                              } for name in to_lookup]

                # batch insert these
                sql, params = self.relatedClass.generate_batch_insert(newRelated)
                cursor = self.relatedClass._meta.database.execute_sql(sql, params)
                idSequence = self.relatedClass._meta.database.last_insert_id(cursor, self.relatedClass)
                # the ids are sequential from this base

                # get/generate the id numbers (that's all we needed anyway)
                for name in self.to_lookup:
                    self.add_cache(name, {
                        'id': idSequence,
                        'name': name
                    })
                    idSequence += 1
                    self.relatives_created += 1

            # now assign them all to the batched records
            for record in self.records:
                for fname, field in self.fields:
                    if fname not in record:
                        continue

                    relatedName = record[fname]
                    related = self.get_cache(relatedName)

                    if not related:
                        raise Exception("What? You can't find %s?" % relatedName)

                    record[fname] = related['id']

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
