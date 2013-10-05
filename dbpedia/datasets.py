"""
This file is responsible for cleaning garbage we don't care about off the
triples provided by DBpedia. For example, url bases.
"""

__all__ = ['article_categories', 'category_labels', 'category_categories']

from resource import DBpediaResource
from ntparser import NTripleParser

DEFAULT_VERSION = '3.9'
DEFAULT_LANGUAGE = 'en'

def url_last_part(url):
    return url.rsplit('/', 1)[1]

class ArticleCategoriesIterator(object):
    def __init__(self, records):
        self.records = records

    def __iter__(self):
        return self

    def next(self):
        # this will throw StopIteration for us if we are out of records
        subject, predicate, object = self.records.next()

        # we expect the predicate to be "subject" in some form or other
        assert predicate.endswith("subject")

        article = url_last_part(subject)
        category = url_last_part(object)

        return article, category

class CategoryLabelIterator(object):
    def __init__(self, records):
        self.records = records

    def __iter__(self):
        return self

    def next(self):
        # this will throw StopIteration for us if we are out of records
        subject, predicate, object = self.records.next()

        # we expect the predicate to be "label" in some form or other
        assert predicate.endswith("label")

        category = url_last_part(subject)
        label = object

        return category, label

class CategoryCategoryIterator(object):
    def __init__(self, records):
        self.records = records

    def __iter__(self):
        return self

    def next(self):
        # this iterator only returns 'broader' relations for now
        found_broader = False
        while not found_broader:

            # this will throw StopIteration for us if we are out of records
            subject, predicate, object = self.records.next()

            if predicate.endswith("broader"):
                found_broader = True

        narrower = url_last_part(subject)
        broader = url_last_part(object)

        return narrower, broader

class TripleCollection(object):

    def __init__(self, resource, iteratorClass):
        self.resource_file = resource.get_file()
        self.iteratorClass = iteratorClass

    def __enter__(self):
        self.resource_file.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.resource_file.__exit__()

    def __iter__(self):
        parser = NTripleParser(self.resource_file)
        return self.iteratorClass(parser.__iter__())


def article_categories(version=DEFAULT_VERSION, language=DEFAULT_LANGUAGE):
    """
    Returns an iterable collection that produces 2-tuples of (article, category).

    For example: ("Achilles", "Category:Characters_in_the_Iliad")

    :param version:
    :param language:
    :return:
    """

    resource = DBpediaResource(dataset="article_categories", version=version, language=language, format="nt")
    return TripleCollection(resource, ArticleCategoriesIterator)


def category_labels(version=DEFAULT_VERSION, language=DEFAULT_LANGUAGE):
    """
    Returns an iterable collection that produces 2-tuples of (category, label).

    For example:
    ("Category:British_monarchs", "British monarchs")

    :param version:
    :param language:
    :return:
    """
    resource = DBpediaResource(dataset="category_labels", version=version, language=language, format="nt")
    return TripleCollection(resource, CategoryLabelIterator)

def category_categories(version=DEFAULT_VERSION, language=DEFAULT_LANGUAGE):
    """
    Returns an iterable collection that produces 2-tuples of category
    pairs like (narrower, broader).

    For example:
    ("Category:World_War_II", "Category:Global_conflicts")

    :param version:
    :param language:
    :return:
    """
    resource = DBpediaResource(dataset="skos_categories", version=version, language=language, format="nt")
    return TripleCollection(resource, CategoryCategoryIterator)

def _test():
    import nose.tools as nt

    expectation = [
        (u'Category:Futurama', u'Futurama'),
        (u'Category:World_War_II', u'World War II'),
        (u'Category:Programming_languages', u'Programming languages'),
        (u'Category:Professional_wrestling', u'Professional wrestling'),
        (u'Category:Algebra', u'Algebra'),
        (u'Category:Anime', u'Anime'),
        (u'Category:Abstract_algebra', u'Abstract algebra'),
        (u'Category:Mathematics', u'Mathematics'),
        (u'Category:Linear_algebra', u'Linear algebra'),
        (u'Category:Calculus', u'Calculus'),
        (u'Category:Monarchs', u'Monarchs'),
        (u'Category:British_monarchs', u'British monarchs'),
    ]

    pairs = 0
    with category_labels() as data:

        for idx, (category, label) in enumerate(data):
            nt.eq_((category, label), expectation[idx])
            pairs += 1

            if idx == len(expectation) - 1:
                break

    nt.eq_(len(expectation), pairs)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)

