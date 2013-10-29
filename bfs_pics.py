"""
Given a root category, generates a series of images
to show the structure of the subcategory tree.
At each iteration of breadth-first search, a new image
is created where the already-traversed nodes are one color,
the newly-traversed nodes are another color, and the un-traversed
nodes are a third color.
"""

import logging
import os, sys
import math
from datetime import datetime
from PIL import Image, ImageDraw
from string import Template

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

def save_image(output_dir, version_path, iteration_name, image):
    """
    Saves the image into the specified path, obviously.
    :param path:
    :param level:
    :param image:
    :return:
    """
    fname = os.path.join(version_path, "%s.png" % iteration_name)
    full_path = os.path.join(output_dir, fname)
    with open(full_path, 'wb') as outfile:
        image.save(outfile)

    return fname

def save_index(path, version_images, root_category, max_depth):
    """
    Generates an HTML file that will display all the images.

    version_images should be a list of tuples where the first element
    is a version-identifier and the second is a list of (paths to images, depth)

    For example:
    version_images = [
        ("v1", [("1/1.png", 1), ("1/2.png", 2)]),
        ("v2", [("2/1.png", 1), ("2/2.png", 2)]),
    ]

    path should be a path to save the index file into.

    :param path:
    :param version_images:
    :return:
    """

    outer_template = Template("""
    <!DOCTYPE HTML>
    <html>
    <head>
        <title>$root subcategories</title>
        <!-- Generated $today -->
        <!-- Depth limit: $max_depth -->
        <link rel="stylesheet" type="text/css" href="//netdna.bootstrapcdn.com/bootstrap/3.0.0/css/bootstrap.min.css"/>
        <style>
        body {
            margin: 20px;
        }
        .images {
            white-space: nowrap;
        }
        </style>
    </head>
    <body>
        <h1>$root subcategories</h1>
        <p class="muted">Generated $today. Depth limited to $max_depth.</p>
        <p>From the root category ($root), subcategories were explored in a breadth-first fashion.
        Each image, from left to right, reveals another level of the hierarchy.
        Each pixel corresponds to a single category. They are ordered by their database id, which may be arbitrary (or not).
        Just-traversed categories are in green, while previously visited categories are in orange.
        DBpedia versions are separated vertically.

        <div class="images">
        $versions
        </div>
    </body>
    </html>
    """)

    version_template = Template("""
    <div class="version">
        <h2>$version</h2>
        <div class="images">
        $images
        </div>
    </div>
    """)

    image_template = Template("""
    <img src="$src" title="$this_level added at depth $depth (total $total; version $version)"/>
    """)

    indexfile = os.path.join(path, 'index.html')
    with open(indexfile, 'wt') as outfile:

        versions = []
        for version_key, image_list in version_images:
            images = []

            for img_path, depth, total, this_level in image_list:
                images.append(
                    image_template.substitute(src=img_path, depth=depth, version=version_key, total=total, this_level=this_level)
                )

            images = "".join(images)
            versions.append(
                version_template.substitute(version=version_key, images=images)
            )

        versions = "".join(versions)
        html = outer_template.substitute(root=root_category, max_depth=max_depth, today=str(datetime.now()), versions=versions)
        outfile.write(html)

    print "Saved %s" % indexfile


def bfs_pics(root_name, depth, output_dir, db):
    models.database_proxy.initialize(db)

    root = Category.select().where(Category.name==root_name).first()

    versions = DataSetVersion.select()

    version_images = []
    for version in versions:
        print "Generating images for version %d..." % (version.id)
        sys.stdout.flush()
        
        current_version_images = []

        # make the output directory
        version_path = str(version.id)
        dirname = os.path.join(output_dir, version_path)

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
        descendants = bfs.descendants(root, norepeats=True, max_levels=depth, version=version)

        total_traversed = 0
        current_level = 0
        frontier = []
        prev_frontier = []
        image = create_image()

        for cat in descendants:
            level = descendants.current_level
            if level != current_level:
                print "Traversed %d categories at level %d" % (len(frontier), current_level)

                total_traversed += len(frontier)
                update_image(image, frontier, prev_frontier)
                fname = save_image(output_dir, version_path, current_level, image)
                current_version_images.append((fname, current_level, total_traversed, len(frontier)))

                prev_frontier = frontier
                frontier = []
                current_level = level

            frontier.append(cat)

        if len(frontier):
            print "Traversed %d categories at level %d" % (len(frontier), current_level)

            # any last few to save
            total_traversed += len(frontier)
            update_image(image, frontier, prev_frontier)
            fname = save_image(output_dir, version_path, current_level, image)
            current_version_images.append((fname, current_level, total_traversed, len(frontier)))

        version_key = "%d: %s (%s)" % (version.id, version.version, str(version.date))
        version_images.append((version_key, current_version_images))

    save_index(output_dir, version_images, root_name, depth)

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    import unicodedata, re
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^:\w\s-]', '', value).strip().lower())
    return re.sub('[-:\s]+', '-', value)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Copy a sample into a second database.")
    common.add_database_args(parser)
    common.add_io_args(parser)

    parser.add_argument("root_category",
                        help="Name of root category")

    parser.add_argument("--output",
                        default=None,
                        required=False,
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

    if args.output is None:
        output = slugify(unicode(args.root_category))
        print "Saving to %s" % output
    else:
        output = args.output

    bfs_pics(root_name=args.root_category, depth=args.depth, output_dir=output, db=db)
