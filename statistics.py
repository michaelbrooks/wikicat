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
from common import timer

import logging
import common
import sys

log = logging.getLogger('statistics')

def calculate_stats(iterations, db, reset=False):

    models.database_proxy.initialize(db)

    models.create_table(CategoryStats, drop_if_exists=False, set_engine='InnoDB')

    versions = DataSetVersion.select()

    # make sure there is a stats entry for every category, for every version
    missing_cats = """
    SELECT COUNT(*)
    FROM categories c
    JOIN dataset_versions v
    LEFT JOIN category_stats cs
        ON c.id = cs.category_id
        AND cs.version_id = v.id
    WHERE cs.id IS NULL
    """
    missing = Category.raw(missing_cats).scalar()
    log.info("Missing stats for %d categories", missing)
    if missing > 0:

        # Create default stats for every category not yet included
        insert_select = """
        INSERT INTO category_stats (version_id, category_id, articles, subcategories, subcategories_reporting)
            SELECT v.id, c.id, 0, 0, 0
            FROM categories c
            JOIN dataset_versions v
            LEFT JOIN category_stats cs
                ON c.id = cs.category_id
                AND cs.version_id = v.id
            WHERE cs.id IS NULL
        """
        log.info('Creating missing stats')
        with timer:
            cursor = db.execute_sql(insert_select)
            db.commit()
        log.info('Created %d entries (%fs)', cursor.rowcount, timer.elapsed())

    if reset:
        reset_stats(db)

    set_immediate_articles(db)
    set_immediate_subcategories(db)
    set_baseline_stats(db)

    calculate_totals(db, iterations)


def reset_stats(db):
    """
    Sets all stats values to NULL.

    :param db:
    :return:
    """
    # reset stats for this version
    log.info('Resetting stats')
    with timer:
        updated = CategoryStats.reset() \
            .execute()
        db.commit()
    log.info("Reset %d entries (%fs)", updated, timer.elapsed())

def set_immediate_articles(db):
    """
    Set num articles to baseline for all childless categories.
    :param db:
    :return:
    """

    # Calculate every category's immediate article counts
    update_immediate_articles = """
    UPDATE category_stats cs
    JOIN (
        SELECT ac.category_id, ac.version_id, COUNT(*) as article_count
        FROM article_categories ac
        -- ignore records that already have an article count
        -- (meaning that this will not update things if anything changes)
        GROUP BY ac.category_id, ac.version_id
    ) sub
    ON sub.category_id = cs.category_id
        AND sub.version_id = cs.version_id
    SET cs.articles = sub.article_count;
    """

    log.info('Initializing articles counts')
    with timer:
        cursor = db.execute_sql(update_immediate_articles)
        db.commit()
    log.info('Updated %d entries (%fs)', cursor.rowcount, timer.elapsed())

    # make sure any remaining values are set to 0
    ensure_zeros = """
    UPDATE category_stats cs
    SET cs.articles = 0
    WHERE cs.articles IS NULL
    """
    log.info('Zeroing unmatched articles counts')
    with timer:
        cursor = db.execute_sql(ensure_zeros)
        db.commit()
    log.info('Updated %d entries (%fs)', cursor.rowcount, timer.elapsed())

def set_immediate_subcategories(db):
    """
    Get the number of immediate subcategories
    :param db:
    :return:
    """

    # Calculate every category's immediate article counts
    update_immediate_categories = """
    UPDATE category_stats cs
    JOIN (
        SELECT cc.version_id, cc.broader_id, COUNT(*) as category_count
        FROM category_categories cc
        -- skip self-referencing category_categories
        WHERE cc.narrower_id != cc.broader_id
        GROUP BY cc.version_id, cc.broader_id
    ) sub
        ON sub.broader_id = cs.category_id
        AND sub.version_id = cs.version_id
    SET cs.subcategories = sub.category_count;
    """

    log.info('Initializing subcategory counts')
    with timer:
        cursor = db.execute_sql(update_immediate_categories)
        db.commit()
    log.info('Updated %d entries (%fs)', cursor.rowcount, timer.elapsed())

    # make sure any remaining values are set to 0
    ensure_zeros = """
    UPDATE category_stats cs
    SET cs.subcategories = 0
    WHERE cs.subcategories IS NULL;
    """
    log.info('Zeroing unmatched subcategories counts')
    with timer:
        cursor = db.execute_sql(ensure_zeros)
        db.commit()
    log.info('Updated %d entries (%fs)', cursor.rowcount, timer.elapsed())

def set_baseline_stats(db):
    """
    Update all the bottom-level categories with no children and no counts
    Should only be called after the the immediate article
    counts have been calculated.
    :param db:
    """

    update_zero = """
    UPDATE category_stats cs
    -- get all subcategories
    SET cs.total_articles = cs.articles,
        cs.total_categories = 0
    WHERE cs.subcategories = 0
        AND (cs.total_articles IS NULL OR
             cs.total_categories IS NULL);
    """

    log.info('Initializing total_articles and total_categories baselines')
    with timer:
        cursor = db.execute_sql(update_zero)
        db.commit()
    log.info('Updated %d entries (%fs)', cursor.rowcount, timer.elapsed())

def calculate_totals(db, iterations):
    """
    Sets all childless categories to have 0 num_categories.
    Then iteratively builds num_categories by walking
    up the tree. Assumes all subcategories contain distinct sets of categories :(

    :param db:
    :param iterations:
    :return:
    """

    # based on what we've calculated so far, sum and propagate to next level
    expand = """
    UPDATE category_stats st
    JOIN (
        SELECT cc.version_id, cc.broader_id,
            SUM(sub_cs.total_categories) AS child_categories,
            SUM(sub_cs.total_articles) AS child_articles,
            COUNT(sub_cs.id) AS reporting
        FROM category_categories cc
        JOIN category_stats sub_cs
            ON sub_cs.category_id = cc.narrower_id
            AND sub_cs.version_id = cc.version_id
        WHERE sub_cs.total_categories IS NOT NULL
        -- (note that total_categories is NULL iff total_articles is NULL)
        GROUP BY cc.version_id, cc.broader_id
    ) sub
        ON st.category_id = sub.broader_id
        AND st.version_id = sub.version_id
    SET st.total_categories = sub.child_categories + st.subcategories,
        st.total_articles = sub.child_articles + st.articles,
        st.subcategories_reporting = sub.reporting
    WHERE st.subcategories_reporting < st.subcategories
    """

    updated = 1
    for i in range(iterations):
        if updated == 0:
            log.warn('Stopping because nothing changed.')
            break

        log.info('Propagating total_categories and total_articles (iteration %d / %d)', i + 1, iterations)
        with timer:
            cursor = db.execute_sql(expand)
            db.commit()
        updated = cursor.rowcount
        log.info('Updated %d entries (%fs)', updated, timer.elapsed())

    remaining = CategoryStats.select(fn.Count(CategoryStats.id))\
        .where(CategoryStats.total_categories >> None)\
        .scalar()

    log.warn("Unfilled stats remaining: %d", remaining)

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
