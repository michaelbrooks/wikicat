"""
This script calculates statistics about the categories in the dataset.
"""

import time

from peewee import JOIN_LEFT_OUTER, fn

from catdb import models
from catdb import mysql
from catdb.mysql import DEFAULT_PASSWORD
from catdb.models import Category, CategoryCategory, CategoryStats, DataSetVersion
from catdb import bfs

import logging
import common
import sys

log = logging.getLogger('statistics')

def calculate_stats(iterations, db, reset=False):

    models.database_proxy.initialize(db)

    models.create_table(CategoryStats, drop_if_exists=False, set_engine='InnoDB')

    versions = DataSetVersion.select()

    for version in versions:

        # make sure there is a stats entry for every category
        missing_cats = """
        SELECT COUNT(*)
        FROM categories c
        LEFT JOIN category_stats cs
            ON c.id = cs.category_id
            AND cs.version_id = %s
        WHERE cs.id IS NULL
        """
        missing = Category.raw(missing_cats, version.id).scalar()
        log.info("Missing stats for %d categories for version %d", missing, version.id)
        if missing > 0:

            # Create default stats for every category not yet included
            insert_select = """
            INSERT INTO category_stats (version_id, category_id)
                SELECT %s, c.id
                FROM categories c
                LEFT JOIN category_stats cs
                    ON c.id = cs.category_id
                    AND cs.version_id = %s
                WHERE cs.id IS NULL
            """
            log.info('Creating empty stats for version %d', version.id)
            cursor = db.execute_sql(insert_select, [version.id, version.id])
            log.info('Created %d entries', cursor.rowcount)
            db.commit()

        if reset:
            reset_stats(db, version)

        num_categories_baselines(db, version)
        calculate_num_categories(iterations, db, version)

        num_articles_baselines(db, version)
        calculate_num_articles(iterations, db, version)

def reset_stats(db, version):
    """
    Sets all stats values to NULL.

    :param db:
    :param version:
    :return:
    """
    # reset stats for this version
    log.info('Resetting stats for version %d', version.id)
    updated = CategoryStats.update(num_articles=None, num_categories=None) \
        .where(CategoryStats.version == version) \
        .execute()
    log.info("Reset %d entries", updated)

def num_articles_baselines(db, version):
    """
    Set num articles to baseline for all childless categories.
    :param db:
    :param version:
    :return:
    """

    # Update all the categories with no children and no counts
    update_zero = """
    UPDATE category_stats st
    JOIN (
        SELECT cs.id, COUNT(ac.id) as article_count
        FROM category_stats cs
        LEFT JOIN article_categories ac
            ON ac.category_id = cs.category_id
            AND ac.version_id = %s
        LEFT JOIN category_categories cc
            ON cc.broader_id = cs.category_id
            AND cc.version_id = %s
            AND cc.narrower_id != cs.category_id
        WHERE cc.id IS NULL
            AND cs.version_id = %s
            AND cs.num_articles IS NULL
        GROUP BY cs.id
    ) sub ON sub.id = st.id
    SET st.num_articles = sub.article_count
    """
    log.info('Initializing num_articles to baselines for version %d', version.id)
    cursor = db.execute_sql(update_zero, [version.id, version.id, version.id])
    log.info('Updated %d entries', cursor.rowcount)
    db.commit()

def num_categories_baselines(db, version):
    """
    Set num categories to baselines for all childless categories.
    :param db:
    :param version:
    :return:
    """

    # Update all the categories with no children and no counts
    update_zero = """
    UPDATE category_stats cs
    LEFT JOIN category_categories cc
        ON cc.broader_id = cs.category_id
        AND cc.version_id = %s
        AND cc.narrower_id != cs.category_id
    SET cs.num_categories = 0
        WHERE cc.id IS NULL
        AND cs.version_id = %s
        AND cs.num_categories IS NULL
    """
    log.info('Initializing num_categories to zero for version %d', version.id)
    cursor = db.execute_sql(update_zero, [version.id, version.id])
    log.info('Updated %d entries', cursor.rowcount)
    db.commit()

def calculate_num_categories(iterations, db, version):
    """
    Sets all childless categories to have 0 num_categories.
    Then iteratively builds num_categories by walking
    up the tree. Assumes all subcategories contain distinct sets of categories :(

    :param iterations:
    :param db:
    :param version:
    :return:
    """

    # based on what we've calculated so far, sum and propagate to next level
    expand = """
    UPDATE category_stats st
    JOIN (
        SELECT cs.id,
            SUM(sub_cs.num_categories + 1) AS child_categories
        FROM category_stats cs
        JOIN category_categories cc
            ON cc.broader_id = cs.category_id
            AND cc.version_id = %s
        JOIN category_stats sub_cs
            ON cc.narrower_id = sub_cs.category_id
            AND sub_cs.num_categories IS NOT NULL
            AND sub_cs.version_id = %s
        WHERE cs.version_id = %s
        AND cs.num_categories IS NULL
        GROUP BY cs.id
    ) sub ON sub.id = st.id
    SET st.num_categories = sub.child_categories
    """

    for i in range(iterations):
        log.info('Propagating num_categories. Iteration %d', i + 1)
        cursor = db.execute_sql(expand, [version.id, version.id, version.id])
        log.info('Updated %d entries', cursor.rowcount)
        db.commit()

    remaining = CategoryStats.select(fn.Count(CategoryStats.id))\
        .where(CategoryStats.num_categories >> None)\
        .scalar()

    log.warn("num_categories is NULL on %d categories in version %d", remaining, version.id)

def calculate_num_articles(iterations, db, version):
    """
    Iteratively builds num_articles by walking
    up the tree. Assumes all subcategories contain distinct sets of articles :(

    :param iterations:
    :param db:
    :param version:
    :return:
    """

    # based on what we've calculated so far, sum and propagate to next level
    expand = """
    UPDATE category_stats st
    JOIN (
        SELECT cs.id,
            SUM(sub_cs.num_articles)
             + COUNT(ac.article_id) AS child_articles
        FROM category_stats cs
        JOIN category_categories cc
            ON cc.broader_id = cs.category_id
            AND cc.version_id = %s
        JOIN category_stats sub_cs
            ON cc.narrower_id = sub_cs.category_id
            AND sub_cs.num_articles IS NOT NULL
            AND sub_cs.version_id = %s
        LEFT JOIN article_categories ac
            ON ac.category_id = cs.category_id
            AND ac.version_id = %s
        WHERE cs.version_id = %s
        AND cs.num_articles IS NULL
        GROUP BY cs.id
    ) sub ON sub.id = st.id
    SET st.num_articles = sub.child_articles
    """

    for i in range(iterations):
        log.info('Propagating num_articles. Iteration %d', i + 1)
        cursor = db.execute_sql(expand, [version.id, version.id, version.id, version.id])
        log.info('Updated %d entries', cursor.rowcount)
        db.commit()

    remaining = CategoryStats.select(fn.Count(CategoryStats.id)) \
        .where(CategoryStats.num_articles >> None) \
        .scalar()

    log.warn("num_articles is NULL on %d categories in version %d", remaining, version.id)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calculate category statistics.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("--iterations",
                        default=5,
                        type=int,
                        required=False,
                        help="Number of times to iterate")

    parser.add_argument("--reset",
                        default=False,
                        action="store_true",
                        help="Reset statistics")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    if args.password:
        password = common.get_database_password(args.user, args.hostname, args.port)
    else:
        password = DEFAULT_PASSWORD

    db = mysql.connect(database=args.database,
                       user=args.user, host=args.hostname,
                       port=args.port, password=password)

    #mysql.trap_warnings()

    if not db:
        exit(1)

    if args.yes:
        models.use_confirmations(False)

    calculate_stats(iterations=args.iterations, db=db, reset=args.reset)
