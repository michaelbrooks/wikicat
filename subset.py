"""
This runnable script is meant to generate a second dataset
that is a smaller subset of the primary dataset.

A subset is defined by a root category.
All subcategories (to some fixed distance)
and their pages will be added to the subset.
"""
import time
import _mysql_exceptions

from catdb import models
from catdb import mysql
from catdb.mysql import DEFAULT_PASSWORD
from catdb.models import Category
from catdb import bfs

import logging
import common
import sys

class Batcher(object):

    def __init__(self, db_from, db_to, limit = 10000):
        self.db_from = db_from
        self.db_to = db_to

        self.num_versions = 0
        self.num_categories = 0
        self.num_articles = 0
        self.num_article_categories = 0
        self.num_category_categories = 0
        self.num_category_labels = 0
        self.submissions = 0

        self.limit = limit
        self.init_batch()

    def init_batch(self):
        self.versions = []
        self.articles = []
        self.categories = []
        self.article_categories = []
        self.category_categories = []
        self.category_labels = []

    def max_len(self):
        return max(len(self.versions),
                   len(self.articles),
                   len(self.categories),
                   len(self.article_categories),
                   len(self.category_categories),
                   len(self.category_labels))

    def is_full(self):
        return  self.max_len() > self.limit

    def submit(self):
        if self.max_len == 0:
            return

        models.database_proxy.initialize(self.db_to)

        cur = models.DataSetVersion.batch_insert(self.versions, ignore=True)
        if cur:
            self.num_versions += cur.rowcount

        cur = models.Category.batch_insert(self.categories, ignore=True)
        if cur:
            self.num_categories += cur.rowcount

        cur = models.Article.batch_insert(self.articles, ignore=True)
        if cur:
            self.num_articles += cur.rowcount

        cur = models.CategoryLabel.batch_insert(self.category_labels, ignore=True)
        if cur:
            self.num_category_labels += cur.rowcount

        cur = models.ArticleCategory.batch_insert(self.article_categories, ignore=True)
        if cur:
            self.num_article_categories += cur.rowcount

        cur = models.CategoryCategory.batch_insert(self.category_categories, ignore=True)
        if cur:
            self.num_category_categories += cur.rowcount

        models.database_proxy.initialize(self.db_from)

        self.init_batch()
        self.submissions += 1

def copy_subset(root_name, depth, db_from, db_to):

    print "Copying all data under root '%s' up to depth %s from '%s' to '%s'" %(root_name, depth, db_from.database, db_to.database)

    models.DataSetVersion._meta.auto_increment = False
    models.Category._meta.auto_increment = False
    models.Article._meta.auto_increment = False
    models.ArticleCategory._meta.auto_increment = False
    models.CategoryCategory._meta.auto_increment = False
    models.CategoryLabel._meta.auto_increment = False

    # first initialize the target database
    models.database_proxy.initialize(db_to)
    models.create_tables(drop_if_exists=True, set_engine="InnoDB")

    # point all the models at the source database
    models.database_proxy.initialize(db_from)

    max_depth = 0
    
    before = time.time()
    last_time = before

    batch = Batcher(db_from, db_to)

    root = Category.select().where(Category.name==root_name).dicts().first()
    if root:
        batch.categories.append(root)
        root = Category(**root)
    else:
        raise Exception("No such category")

    # copy over all the dataset versions
    versions = models.DataSetVersion.select().dicts().execute()
    for ver in versions:
        batch.versions.append(ver)

    batch.category_labels.extend(root.get_labels().dicts().execute())
    batch.articles.extend(root.get_articles().dicts().execute())
    batch.article_categories.extend(root.get_article_categories().dicts().execute())

    category_categories = bfs.descendant_links(root, norepeats=True, max_levels=depth)
    for cat_cat in category_categories:
        max_depth = max(max_depth, category_categories.current_level)

        batch.category_categories.append(cat_cat._data)

        leaf_cat = Category.select().where(Category.id == cat_cat.narrower).dicts().first()
        batch.categories.append(leaf_cat)

        leaf_cat = Category(**leaf_cat)

        batch.category_labels.extend(leaf_cat.get_labels().dicts().execute())
        batch.articles.extend(leaf_cat.get_articles().dicts().execute())
        batch.article_categories.extend(leaf_cat.get_article_categories().dicts().execute())

        if batch.is_full():
            batch.submit()
            db_to.commit()

            if time.time() - last_time > 30:
                print "Copied %d versions with %d categories (%d labels); %d articles; %d article_categories, and %d category_categories" \
                      % (batch.num_versions, batch.num_categories, batch.num_category_labels, batch.num_articles, batch.num_article_categories, batch.num_category_categories)
                print "Time taken: %fs. Maximum depth %d. %d batches." %(after - before, max_depth, batch.submissions)
                sys.stdout.flush

                last_time = time.time()

    batch.submit()
    db_to.commit()

    after = time.time()

    print "Copied %d versions with %d categories (%d labels); %d articles; %d article_categories, and %d category_categories" \
          % (batch.num_versions, batch.num_categories, batch.num_category_labels, batch.num_articles, batch.num_article_categories, batch.num_category_categories)
    print "Time taken: %fs. Maximum depth %d. %d batches." %(after - before, max_depth, batch.submissions)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Copy a sample into a second database.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("root_category",
                        help="Name of root category")

    parser.add_argument("--depth",
                        default=2,
                        type=int,
                        required=False,
                        help="Category depth to explore from root category")

    parser.add_argument("--target",
                        required=True,
                        help="The database to write into")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if args.password:
        password = common.get_database_password(args.user, args.hostname, args.port)
    else:
        password = DEFAULT_PASSWORD

    if args.database == args.target:
        print "Source and target database cannot be the same."

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.hostname,
                       port=args.port, password=password)

    #mysql.trap_warnings()

    if not db:
        exit(1)

    target = mysql.connect(database=args.target,
                           user=args.user, host=args.hostname,
                           port=args.port, password=password)
    if not target:
        exit(1)

    if args.yes:
        models.use_confirmations(False)

    copy_subset(root_name=args.root_category, depth=args.depth, db_from=db, db_to=target)
