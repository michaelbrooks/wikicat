"""
This file is responsible for cleaning garbage we don't care about off the
triples provided by DBpedia. For example, url bases.
"""

__all__ = ['get_collection', 'DEFAULT_VERSION', 'DEFAULT_LANGUAGE']

from resource import DBpediaResource
from ntparser import NTripleParser

DEFAULT_VERSION = '3.9'
DEFAULT_LANGUAGE = 'en'
DEFAULT_URL_BASE = "http://dbpedia.org/resource/"

def url_last_part(url, url_base=DEFAULT_URL_BASE):
    if not url.startswith(url_base):
        raise Exception("Unexpected URL %s" % url)

    return url[len(url_base):]

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

        return {
            "article": article,
            "category": category
        }

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

        return {
            "category": category,
            "label": label
        }

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

        return {
            "narrower": narrower,
            "broader": broader
        }

class TripleCollection(object):

    def __init__(self, resource, iteratorClass):
        self.resource = resource
        self.iteratorClass = iteratorClass

    def __enter__(self):
        self.resource_file = self.resource.get_file()
        self.resource_file.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.resource_file.__exit__()

    def __iter__(self):
        parser = NTripleParser(self.resource_file)
        return self.iteratorClass(parser.__iter__())

iterator_mapping = {
    'article_categories': ArticleCategoriesIterator,
    'category_categories': CategoryCategoryIterator,
    'category_labels': CategoryLabelIterator
}

def get_collection(dataset, version=DEFAULT_VERSION, language=DEFAULT_LANGUAGE):
    if dataset not in iterator_mapping:
        raise Exception("No iterator for %s" % dataset)
    resource = DBpediaResource(dataset=dataset, version=version, language=language, format="nt")
    iterator = iterator_mapping[dataset]
    return TripleCollection(resource, iterator)

def _test():
    import nose.tools as nt

    expectation = [
        {'category': u'Category:Futurama', 'label': u'Futurama'},
        {'category': u'Category:World_War_II', 'label': u'World War II'},
        {'category': u'Category:Programming_languages', 'label': u'Programming languages'},
        {'category': u'Category:Professional_wrestling', 'label': u'Professional wrestling'},
        {'category': u'Category:Algebra', 'label': u'Algebra'},
        {'category': u'Category:Anime', 'label': u'Anime'},
        {'category': u'Category:Abstract_algebra', 'label': u'Abstract algebra'},
        {'category': u'Category:Mathematics', 'label': u'Mathematics'},
        {'category': u'Category:Linear_algebra', 'label': u'Linear algebra'},
        {'category': u'Category:Calculus', 'label': u'Calculus'},
        {'category': u'Category:Monarchs', 'label': u'Monarchs'},
        {'category': u'Category:British_monarchs', 'label': u'British monarchs'},
    ]

    pairs = 0
    with get_collection('category_labels') as data:

        for idx, record in enumerate(data):
            nt.eq_(record, expectation[idx])

            pairs += 1

            if idx == len(expectation) - 1:
                break

    nt.eq_(len(expectation), pairs)

    # Test some urls with slashes in them
    tripleTest = [
         ('http://dbpedia.org/resource/Category:2009_Fed_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '2009 Fed Cup Europe/Africa Zone'),
         ('http://dbpedia.org/resource/Category:2009_Davis_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '2009 Davis Cup Europe/Africa Zone'),
         ('http://dbpedia.org/resource/Category:2010_Fed_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '2010 Fed Cup Europe/Africa Zone'),
         ('http://dbpedia.org/resource/Category:2011_Fed_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '2011 Fed Cup Europe/Africa Zone'),
         ('http://dbpedia.org/resource/Category:1992_Federation_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '1992 Federation Cup Europe/Africa Zone'),
         ('http://dbpedia.org/resource/Category:1993_Federation_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '1993 Federation Cup Europe/Africa Zone'),
         ('http://dbpedia.org/resource/Category:1994_Federation_Cup_Europe/Africa_Zone', 'http://www.w3.org/2000/01/rdf-schema#label', '1994 Federation Cup Europe/Africa Zone'),
    ]
    expectation = [
        {'category': 'Category:2009_Fed_Cup_Europe/Africa_Zone', 'label': '2009 Fed Cup Europe/Africa Zone'},
        {'category': 'Category:2009_Davis_Cup_Europe/Africa_Zone', 'label': '2009 Davis Cup Europe/Africa Zone'},
        {'category': 'Category:2010_Fed_Cup_Europe/Africa_Zone', 'label': '2010 Fed Cup Europe/Africa Zone'},
        {'category': 'Category:2011_Fed_Cup_Europe/Africa_Zone', 'label': '2011 Fed Cup Europe/Africa Zone'},
        {'category': 'Category:1992_Federation_Cup_Europe/Africa_Zone', 'label': '1992 Federation Cup Europe/Africa Zone'},
        {'category': 'Category:1993_Federation_Cup_Europe/Africa_Zone', 'label': '1993 Federation Cup Europe/Africa Zone'},
        {'category': 'Category:1994_Federation_Cup_Europe/Africa_Zone', 'label': '1994 Federation Cup Europe/Africa Zone'}
    ]

    iter = CategoryLabelIterator(tripleTest.__iter__())

    pairs = 0
    for idx, result in enumerate(iter):
        pairs += 1
        nt.eq_(result, expectation[idx])

    nt.eq_(pairs, len(expectation))

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)

