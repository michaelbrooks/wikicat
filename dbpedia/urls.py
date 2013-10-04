#
# The 'build()' function, defined in this file,
# is in charge of generating urls to DBPedia files.
# Files are specified by data set, version, language, and format.
#
__all__ = ['build', 'canonical_datasets']

from string import Template

# For reference, the dataset names that should be used as input
canonical_datasets = [
    'article_categoreis',
    'category_labels',
    'skos_categories'
]

class VersionHandler(object):
    """
    A base version handler that works for the modern dataset (3.9 at time of writing).
    """

    url_template = Template("http://downloads.dbpedia.org/${version}/${language}/${dataset}_${language}.${format}.bz2")
    dataset_map = {}

    def __init__(self, version='3.9'):
        self.version = version

    def build(self, dataset, language="en", format="nt"):

        if dataset in self.dataset_map:
            dataset = self.dataset_map[dataset]

        return self.url_template.substitute(dataset=dataset,
                                       version=self.version,
                                       language=language,
                                       format=format)

class VersionHandler_2_0(VersionHandler):
    """
    A version handler for the 2.0 dataset which used
    different dataset names and are stored in a different place.
    """

    url_template = Template("http://dbpedia.org/docs/downloads/${version}/${dataset}.${format}.bz2")

    dataset_map = {
        'article_categories': 'articles_category',
        'category_labels': 'categories_label',
        'skos_categories': 'categories_skos'
    }

    def __init__(self, version="2007-08-30"):
        super(VersionHandler_2_0, self).__init__(version=version)

class VersionHandler_3_0(VersionHandler):
    """
    A version handler for 3.0 and several other versions after that
    which used the same dataset naming conventions.
    """

    dataset_map = {
        'article_categories': 'articlecategories',
        'category_labels': 'categorylabels',
        'skos_categories': 'skoscategories'
    }

    def __init__(self, version='3.0'):
        super(VersionHandler_3_0, self).__init__(version=version)


# Map from DBpedia versions to url generators
version_map = {
    '3.9': VersionHandler(),
    '3.0': VersionHandler_3_0(),
    '3.1': VersionHandler_3_0(version='3.1'),
    '3.2': VersionHandler_3_0(version='3.2'),
    '3.3': VersionHandler_3_0(version='3.3'),
    '3.4': VersionHandler_3_0(version='3.4'),
    '2.0': VersionHandler_2_0(),
}

def build(resource):
    """
    Generate the DBpedia url for the given resource.

    :param resource:
    :return:
    """

    if resource.version in version_map:
        handler = version_map[resource.version]
    else:
        handler = VersionHandler(version=resource.version)

    return handler.build(dataset=resource.dataset,
                         language=resource.language,
                         format=resource.format)

def _test_urls():
    import nose.tools as nt
    from resource import DBpediaResource as res

    # test the core 3.9 urls
    nt.assert_equal(build(res("article_categories", "3.9", "en", "nt")),
           "http://downloads.dbpedia.org/3.9/en/article_categories_en.nt.bz2")
    nt.assert_equal(build(res("category_labels", "3.9", "en", "nt")),
           "http://downloads.dbpedia.org/3.9/en/category_labels_en.nt.bz2")
    nt.assert_equal(build(res("skos_categories", "3.9", "en", "nt")),
           "http://downloads.dbpedia.org/3.9/en/skos_categories_en.nt.bz2")

    # go way back and test the 2.0 urls
    nt.assert_equal(build(res("article_categories", "2.0", "en", "nt")),
                    "http://dbpedia.org/docs/downloads/2007-08-30/articles_category.nt.bz2")
    nt.assert_equal(build(res("category_labels", "2.0", "en", "nt")),
                    "http://dbpedia.org/docs/downloads/2007-08-30/categories_label.nt.bz2")
    nt.assert_equal(build(res("skos_categories", "2.0", "en", "nt")),
                    "http://dbpedia.org/docs/downloads/2007-08-30/categories_skos.nt.bz2")

    # try several versions of the article_categories dataset
    nt.assert_equal(build(res("article_categories", "3.8", "en", "nt")),
                    "http://downloads.dbpedia.org/3.8/en/article_categories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.7", "en", "nt")),
                    "http://downloads.dbpedia.org/3.7/en/article_categories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.6", "en", "nt")),
                    "http://downloads.dbpedia.org/3.6/en/article_categories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.5.1", "en", "nt")),
                    "http://downloads.dbpedia.org/3.5.1/en/article_categories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.5", "en", "nt")),
                    "http://downloads.dbpedia.org/3.5/en/article_categories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.4", "en", "nt")),
                    "http://downloads.dbpedia.org/3.4/en/articlecategories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.3", "en", "nt")),
                    "http://downloads.dbpedia.org/3.3/en/articlecategories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.1", "en", "nt")),
                    "http://downloads.dbpedia.org/3.1/en/articlecategories_en.nt.bz2")
    nt.assert_equal(build(res("article_categories", "3.0", "en", "nt")),
                    "http://downloads.dbpedia.org/3.0/en/articlecategories_en.nt.bz2")


if __name__ == "__main__":
    import sys
    try:
        _test_urls()
        print "Tests Passed"
    except AssertionError as e:
        print >> sys.stderr, "ERROR: TESTS FAILED"
        print >> sys.stderr, e
