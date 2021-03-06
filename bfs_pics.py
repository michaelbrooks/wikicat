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
from datetime import datetime
from string import Template

import common
from catdb import models
from catdb.models import Category, DataSetVersion
from catdb import mysql
from catdb.mysql import DEFAULT_PASSWORD
from catdb import bfs
from dbpedia import resource
from output.bitmap import BitMap


def save_index(path, version_images, root_category, max_depth, order='id'):
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
            background: #333;
            color: #eee;
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
        Each pixel corresponds to a single category. Pixels are ordered by $order.
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

    indexfile = os.path.join(path, 'images_by_%s.html' % order)
    with open(indexfile, 'wt') as outfile:

        versions = []
        for version_dict in version_images:
            version = version_dict['version']
            frames = version_dict['frames']

            version_key = "%d: %s (%s)" % (version['id'],
                                           version['version'],
                                           str(version['date']))
            images = []

            for frame in frames:
                img_path = frame['img']
                depth = frame['depth']
                total = frame['total_traversed']
                this_level = frame['new_traversed']

                images.append(
                    image_template.substitute(src=img_path,
                                              depth=depth,
                                              version=version_key,
                                              total=total,
                                              this_level=this_level)
                )

            images = "".join(images)
            versions.append(
                version_template.substitute(version=version_key, images=images)
            )

        versions = "".join(versions)
        html = outer_template.substitute(root=root_category.name,
                                         max_depth=max_depth,
                                         today=str(datetime.now()),
                                         versions=versions,
                                         order=order)
        outfile.write(html)

    datafile = os.path.join(path, 'data.json')
    with open(datafile, 'wt') as outfile:
        data = {
            'version_images': version_images,
            'max_depth': max_depth,
            'generated': str(datetime.now()),
            'root': root_category.id,
            'root_name': root_category.name,
            'order': order
        }

        import json
        outfile.write(json.dumps(data, sort_keys=True, indent=3))

    print "Saved %s and %s" % (indexfile, datafile)

class IterationTracker(object):

    def __init__(self, output_dir, version_path, order):
        self.output_dir = output_dir
        self.version_path = version_path
        self.order = order
        self.total_traversed = 0
        self.current_depth = 0
        self.frontier = []
        self.prev_frontier = []
        self.version_images = []
        total = Category.select().count()
        self.bitmap = BitMap(total)

    def update_image(self):
        """
        Paints the given categories to indicate they have just been traversed.
        Previously traversed categories will be painted another color.
        :param image:
        :param categories:
        :param prev_categories:
        :return:
        """

        interior_color = (162, 99, 47)
        frontier_color = (102, 255, 0)

        # mark the old categories
        if len(self.prev_frontier):
            self.bitmap.color_numbers(self.prev_frontier, interior_color)

        # mark the new categories
        if len(self.frontier):
            self.bitmap.color_numbers(self.frontier, frontier_color)

    def save_image(self, output_dir, version_path, iteration_name):
        """
        Saves the image into the specified path, obviously.
        :param path:
        :param level:
        :param image:
        :return:
        """
        fname = os.path.join(version_path, "%s_by_%s.png" % (iteration_name, self.order))
        full_path = os.path.join(output_dir, fname)
        self.bitmap.save(full_path)
        return fname

    def record_depth(self):
        self.total_traversed += len(self.frontier)
        self.update_image()
        fname = self.save_image(self.output_dir, self.version_path,
                           self.current_depth)

        self.version_images.append({
            'img': fname,
            'depth': self.current_depth,
            'total_traversed': self.total_traversed,
            'new_traversed': len(self.frontier)
        })

        print "Traversed %d categories at depth %d" % (len(self.frontier), self.current_depth)
        sys.stdout.flush()

        self.prev_frontier = self.frontier
        self.frontier = []
        self.current_depth += 1

    def traverse_category(self, cat, depth):
        if depth != self.current_depth:
            self.record_depth()

        if self.order == 'id':
            cat_info = cat.id
        elif self.order == 'added':
            cat_info = self.total_traversed + len(self.frontier)
        else:
            raise Exception("Order must be id or added")

        self.frontier.append(cat_info)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self.frontier):
            self.record_depth()

    def collect(self):
        return self.version_images

def bfs_pics(root_name, depth, output_dir, db, version_list=[]):
    models.database_proxy.initialize(db)

    root = Category.select().where(Category.name==root_name).first()

    versions = DataSetVersion.select()
    if len(version_list):
        versions = versions.where(DataSetVersion.version << version_list)

    version_images = []
    version_images_added = []
    for version in versions:
        print "Generating images for version %s..." % version.version
        sys.stdout.flush()

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
        descendants = bfs.descendant_levels(root, norepeats=True, max_levels=depth, version=version)


        tracker = IterationTracker(output_dir, version_path, order='id')
        tracker_added = IterationTracker(output_dir, version_path, order='added')

        with tracker, tracker_added:
            for level in descendants:
                for cat in level:
                    depth = descendants.current_level
                    tracker.traverse_category(cat, depth)
                    tracker_added.traverse_category(cat, depth)

        version_images.append({
            'version': {
                'id': version.id,
                'version': version.version,
                'date': str(version.date)
            },
            'frames': tracker.collect()
        })

        version_images_added.append({
            'version': {
                'id': version.id,
                'version': version.version,
                'date': str(version.date)
            },
            'frames': tracker_added.collect()
        })

    save_index(output_dir, version_images, root, depth, order='id')
    save_index(output_dir, version_images_added, root, depth, order='added')

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

    parser.add_argument("--versions", "-v",
                        required=False,
                        metavar='DBPEDIA_VERSION',
                        nargs='*',
                        default=[],
                        choices=resource.version_names,
                        help="Which DBpedia version number(s) to import")

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
        output = common.slugify(unicode(args.root_category))
        print "Saving to %s" % output
    else:
        output = args.output

    with common.timer:
        bfs_pics(root_name=args.root_category,
                 depth=args.depth,
                 output_dir=output,
                 db=db,
                 version_list=args.versions)

    print 'Exported complete (%fs)' % common.timer.elapsed()