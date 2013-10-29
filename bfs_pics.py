"""
Given a root category, generates a series of images
to show the structure of the subcategory tree.
At each iteration of breadth-first search, a new image
is created where the already-traversed nodes are one color,
the newly-traversed nodes are another color, and the un-traversed
nodes are a third color.
"""

import logging
import os
import math
from PIL import Image, ImageDraw

import common
from catdb import models
from catdb.models import Category, DataSetVersion
from catdb import mysql
from catdb.mysql import DEFAULT_PASSWORD
from catdb import bfs

def create_image():
    """
    Generates a manipulable in-memory image resource.
    :return:
    """
    # how many categories?
    total = Category.select().count()
    aspect_ratio = float(4) / 3
    width = int(math.sqrt(aspect_ratio * total))
    height = int(width / aspect_ratio)

    img = Image.new("RGB", (width, height))
    return img

def map_category(category, image):
    """
    Maps the category to a pixel in the image.

    :param category:
    :param image:
    :return:
    """
    width = image.size[0]

    row = math.floor(category.id / width)
    col = category.id % width

    return col, row

def update_image(image, categories, prev_categories):
    """
    Paints the given categories to indicate they have just been traversed.
    Previously traversed categories will be painted another color.
    :param image:
    :param categories:
    :param prev_categories:
    :return:
    """

    draw = ImageDraw.Draw(image)
    interior_color = (162, 99, 47)
    frontier_color = (102, 255, 0)

    # mark the old categories
    if len(prev_categories):
        draw.point([map_category(cat, image) for cat in prev_categories], interior_color)

    # mark the new categories
    if len(categories):
        draw.point([map_category(cat, image) for cat in categories], frontier_color)

def save_image(path, iteration_name, image):
    """
    Saves the image into the specified path, obviously.
    :param path:
    :param level:
    :param image:
    :return:
    """
    fname = os.path.join(path, "%s.png" % iteration_name)
    with open(fname, 'wb') as outfile:
        image.save(outfile)

def bfs_pics(root_name, depth, output_dir, db):
    models.database_proxy.initialize(db)

    root = Category.select().where(Category.name==root_name).first()

    versions = DataSetVersion.select()

    for version in versions:
        # make the output directory
        dirname = os.path.join(output_dir, str(version.id))

        try:
            os.makedirs(dirname)
        except OSError:
            if os.path.exists(dirname):
                # We are nearly safe
                pass
            else:
                # There was an error on creation, so make sure we know about it
                raise

        # get the bfs iterator
        descendants = bfs.descendants(root, norepeats=False, max_levels=depth, version=version)

        current_level = 0
        frontier = []
        prev_frontier = []
        image = create_image()

        for cat in descendants:
            level = descendants.current_level
            if level != current_level:
                update_image(image, frontier, prev_frontier)
                save_image(dirname, current_level, image)

                prev_frontier = frontier
                frontier = []
                current_level = level

            frontier.append(cat)

        if len(frontier):
            # any last few to save
            update_image(image, frontier, prev_frontier)
            save_image(dirname, current_level, image)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Copy a sample into a second database.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("root_category",
                        help="Name of root category")

    parser.add_argument("output_dir",
                        help="Output directory for images")

    parser.add_argument("--depth",
                        default=5,
                        type=int,
                        required=False,
                        help="Category depth to explore from root category")

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

    bfs_pics(root_name=args.root_category, depth=args.depth, output_dir=args.output_dir, db=db)
