"""
Creates a Breadth-First-Search iterator over
the category hierarchy in the MySQL database.
"""

from collections import deque
from models import Category

class BFSIterator(object):

    def __init__(self, root, direction="down", norepeats=False, max_levels=None, version=None):
        self.direction = direction
        self.processed = deque()
        self.iterPointer = -1

        self.max_levels = max_levels
        self.norepeats = norepeats
        self.version = version

        self._init_queue(root)

        if norepeats:
            self.checked = set()

    def __iter__(self):
        return self

    def _init_queue(self, root):
        self.queue = deque([(root, 0)])

    def _get_next_level(self, current):
        if self.direction == 'down':
            return current.get_children(version=self.version)
        elif self.direction == 'up':
            return current.get_parents(version=self.version)
        else:
            raise Exception("Unknown direction %s" % self.direction)

    def _traverse(self):

        if not len(self.queue):
            return

        curr, level = self.queue.popleft()

        if self.max_levels is None or level < self.max_levels:
            nextLevel = self._get_next_level(curr)

            if self.norepeats:
                for node in nextLevel:
                    if node.id not in self.checked:
                        self.checked.add(node.id)
                        self.queue.append((node, level + 1))
            else:
                self.queue.extend([(n, level + 1) for n in nextLevel])

        if curr:
            self.current_level = level
            return curr

    def next(self):

        node = self._traverse()

        if node is None:
            raise StopIteration

        return node

class BFSLinkIterator(BFSIterator):

    def _init_queue(self, root):
        initialLinks = root.get_category_categories(direction=self.direction, version=self.version)
        self.queue = deque((link, 0) for link in initialLinks)

    def _get_next_level(self, currentLink):
        if self.direction == 'down':
            node = currentLink.narrower
        elif self.direction == 'up':
            node = currentLink.broader
        else:
            raise Exception("Unknown direction %s" % self.direction)

        return node.get_category_categories(direction=self.direction, version=self.version)

class BFSLevelIterator(BFSIterator):
    """
    Here the queue only ever contains one item, but that item is the entire current level.
    """
    def _init_queue(self, root):
        self.queue = deque([([root], 0)]) # the entire first level as an array is the root

    def _get_next_level(self, current):
        if self.direction == 'down':
            return Category.get_all_children(current, version=self.version)
        elif self.direction == 'up':
            return Category.get_all_parents(current, version=self.version)
        else:
            raise Exception("Unknown direction %s" % self.direction)

    def _traverse(self):

        if not len(self.queue):
            return

        curr, level = self.queue.popleft()

        if self.max_levels is None or level < self.max_levels:
            nextLevel = self._get_next_level(curr)

            if self.norepeats:
                newNodes = []
                for node in nextLevel:
                    if node.id not in self.checked:
                        self.checked.add(node.id)
                        newNodes.append(node)
                nextLevel = newNodes

            if len(nextLevel) > 0:
                self.queue.append((nextLevel, level + 1))

        if curr:
            self.current_level = level
            return curr

def descendants(rootCategory, norepeats=False, max_levels=None, version=None):
    return BFSIterator(rootCategory,
                       direction='down', norepeats=norepeats,
                       max_levels=max_levels, version=version)

def ancestors(rootCategory, norepeats=False, max_levels=None, version=None):
    return BFSIterator(rootCategory,
                       direction='up', norepeats=norepeats,
                       max_levels=max_levels, version=version)

def descendant_levels(rootCategory, norepeats=False, max_levels=None, version=None):
    return BFSLevelIterator(rootCategory,
                            direction='down', norepeats=norepeats,
                            max_levels=max_levels, version=version)

def ancestor_levels(rootCategory, norepeats=False, max_levels=None, version=None):
    return BFSLevelIterator(rootCategory,
                            direction='up', norepeats=norepeats,
                            max_levels=max_levels, version=version)

def descendant_links(rootCategory, norepeats=False, max_levels=None, version=None):
    return BFSLinkIterator(rootCategory,
                           direction='down', norepeats=norepeats,
                           max_levels=max_levels, version=version)

def ancestor_links(rootCategory, norepeats=False, max_levels=None, version=None):
    return BFSLinkIterator(rootCategory,
                           direction='up', norepeats=norepeats,
                           max_levels=max_levels, version=version)

def _test():
    import nose.tools as nt
    import mysql, models, insert

    db = mysql.connect('wikicat', user='root', host='localhost', password='')

    nt.ok_(db)

    models.database_proxy.initialize(db)
    models.use_confirmations(False)
    models.create_tables(drop_if_exists=True)

    # some example data
    dataset = [
        {'broader': u'Animals', 'narrower': u'Mammals'},
        {'broader': u'Animals', 'narrower': u'Birds'},
        {'broader': u'Animals', 'narrower': u'Reptiles'},
        {'broader': u'Mammals', 'narrower': u'Dogs'},
        {'broader': u'Mammals', 'narrower': u'Cats'},
        {'broader': u'Reptiles', 'narrower': u'Lizards'},
        {'broader': u'Reptiles', 'narrower': u'Snakes'},
        {'broader': u'Birds', 'narrower': u'Ostriches'},
        {'broader': u'Birds', 'narrower': u'Penguins'},
        {'broader': u'Birds', 'narrower': u'Eagles'},
        {'broader': u'Cats', 'narrower': u'Lions'},
        {'broader': u'Cats', 'narrower': u'Tigers'}
    ]

    datasetVersion = models.dataset_version(version='3.9', language='en', date='2013-04-03')
    imported = insert.insert_dataset(data=dataset, dataset='category_categories', version_instance=datasetVersion)
    nt.eq_(imported, len(dataset))

    cats = Category.get(Category.name=='Cats')
    typesOfCat = [c.name for c in descendants(cats)]
    typesOfCat.sort()
    nt.eq_(['Cats', 'Lions', 'Tigers'], typesOfCat)

    eagle = Category.get(Category.name=='Eagles')
    eagleParents = [e.name for e in ancestors(eagle)]
    eagleParents.sort()
    nt.eq_(['Animals', 'Birds', 'Eagles'], eagleParents)

    mammals = Category.get(Category.name=='Mammals')
    mammalChildren = [m.name for m in descendants(mammals, max_levels=0)]
    mammalChildren.sort()
    nt.eq_(['Mammals'], mammalChildren)

    mammalChildren = [m.name for m in descendants(mammals, max_levels=1)]
    mammalChildren.sort()
    nt.eq_(['Cats', 'Dogs', 'Mammals'], mammalChildren)

    mammalChildren = [m.name for m in descendants(mammals, max_levels=2)]
    mammalChildren.sort()
    nt.eq_(['Cats', 'Dogs', 'Lions', 'Mammals', 'Tigers'], mammalChildren)

    links = [(f.broader.name, f.narrower.name) for f in descendant_links(mammals)]
    links.sort()
    nt.eq_([(u'Cats', u'Lions'), (u'Cats', u'Tigers'), (u'Mammals', u'Cats'), (u'Mammals', u'Dogs')], links)

    links = [(f.broader.name, f.narrower.name) for f in descendant_links(mammals, max_levels=0)]
    links.sort()
    nt.eq_([(u'Mammals', u'Cats'), (u'Mammals', u'Dogs')], links)

    # now add a node with two parents
    dataset = [
        {'broader': u'Animals', 'narrower': u'Mammals'},
        {'broader': u'Animals', 'narrower': u'Birds'},
        {'broader': u'Animals', 'narrower': u'Reptiles'},
        {'broader': u'Mammals', 'narrower': u'Monotremes'},
        {'broader': u'Reptiles', 'narrower': u'Monotremes'}
    ]

    imported = insert.insert_dataset(data=dataset, dataset='category_categories', version_instance=datasetVersion)
    nt.eq_(imported, len(dataset))

    monotremes = Category.get(Category.name=='Monotremes')
    monoParents = [e.name for e in ancestors(monotremes, norepeats=False)]
    monoParents.sort()
    nt.eq_(['Animals', 'Animals', 'Mammals', 'Monotremes', 'Reptiles'], monoParents)

    monoParents = [e.name for e in ancestors(monotremes, norepeats=True)]
    monoParents.sort()
    nt.eq_(['Animals', 'Mammals', 'Monotremes', 'Reptiles'], monoParents)

    # now add a "second root"
    dataset = [
        {'broader': u'Animals', 'narrower': u'Mammals'},
        {'broader': u'Animals', 'narrower': u'Birds'},
        {'broader': u'Animals', 'narrower': u'Reptiles'},
        {'broader': u'Mammals', 'narrower': u'Dogs'},
        {'broader': u'Mammals', 'narrower': u'Cats'},
        {'broader': u'Reptiles', 'narrower': u'Lizards'},
        {'broader': u'Reptiles', 'narrower': u'Snakes'},
        {'broader': u'Birds', 'narrower': u'Ostriches'},
        {'broader': u'Birds', 'narrower': u'Penguins'},
        {'broader': u'Birds', 'narrower': u'Eagles'},
        {'broader': u'Cats', 'narrower': u'Lions'},
        {'broader': u'Cats', 'narrower': u'Tigers'},
        {'broader': u'Pets', 'narrower': u'Lizards'},
        {'broader': u'Pets', 'narrower': u'Dogs'},
        {'broader': u'Pets', 'narrower': u'Cats'},
    ]
    imported = insert.insert_dataset(data=dataset, dataset='category_categories', version_instance=datasetVersion)
    nt.eq_(imported, len(dataset))

    lizards = Category.get(Category.name=='Lizards')
    lizardParents = [e.name for e in ancestors(lizards)]
    lizardParents.sort()
    nt.eq_(['Animals', 'Lizards', 'Pets', 'Reptiles'], lizardParents)

    reptiles = Category.get(Category.name=='Reptiles')
    reptileTypes = [e.name for e in descendants(reptiles)]
    reptileTypes.sort()
    nt.eq_(['Lizards', 'Reptiles', 'Snakes'], reptileTypes)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)


