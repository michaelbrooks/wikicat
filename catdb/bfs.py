"""
Creates a Breadth-First-Search iterator over
the category hierarchy in the MySQL database.
"""

from collections import deque
from models import Category, CategoryCategory

class BFSIterator(object):

    def __init__(self, root, direction="down", norepeats=False):
        self.queue = deque([root])
        self.processed = deque()
        self.iterPointer = -1

        self.norepeats = norepeats

        if norepeats:
            self.checked = set()

        self.direction = direction

    def __iter__(self):
        return self

    def _traverse(self):
        if not len(self.queue):
            return

        curr = self.queue.popleft()

        if self.direction == 'down':
            nextLevel = curr.get_children()
        elif self.direction == 'up':
            nextLevel = curr.get_parents()
        else:
            raise Exception("Unknown direction %s" % self.direction)

        if self.norepeats:
            for node in nextLevel:
                if node.id not in self.checked:
                    self.checked.add(node.id)
                    self.queue.append(node)
        else:
            self.queue.extend(nextLevel)

        self.processed.append(curr)

    def next(self):
        self.iterPointer += 1

        if self.iterPointer >= len(self.processed):
            self._traverse()

        if self.iterPointer >= len(self.processed):
            raise StopIteration

        return self.processed[self.iterPointer]

def descendants(rootCategory, norepeats=False):
    return BFSIterator(rootCategory, direction='down', norepeats=norepeats)

def ancestors(rootCategory, norepeats=False):
    return BFSIterator(rootCategory, direction='up', norepeats=norepeats)


def _test():
    import nose.tools as nt
    import mysql, models

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
    imported = models.insert_dataset(data=dataset, dataset='category_categories', version_instance=datasetVersion)
    nt.eq_(imported, len(dataset))

    cats = Category.get(Category.name=='Cats')
    typesOfCat = [c.name for c in descendants(cats)]
    typesOfCat.sort()
    nt.eq_(['Cats', 'Lions', 'Tigers'], typesOfCat)

    eagle = Category.get(Category.name=='Eagles')
    eagleParents = [e.name for e in ancestors(eagle)]
    eagleParents.sort()
    nt.eq_(['Animals', 'Birds', 'Eagles'], eagleParents)

    # now add a node with two parents
    dataset = [
        {'broader': u'Animals', 'narrower': u'Mammals'},
        {'broader': u'Animals', 'narrower': u'Birds'},
        {'broader': u'Animals', 'narrower': u'Reptiles'},
        {'broader': u'Mammals', 'narrower': u'Monotremes'},
        {'broader': u'Reptiles', 'narrower': u'Monotremes'}
    ]

    imported = models.insert_dataset(data=dataset, dataset='category_categories', version_instance=datasetVersion)
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
    imported = models.insert_dataset(data=dataset, dataset='category_categories', version_instance=datasetVersion)
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


