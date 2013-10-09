#
# Convenient class for representing dbpedia resources.
#

__all__ = ['DBpediaResource', 'dataset_names', 'version_names']

import bz2file as bz2
import download

dataset_names = [
    'category_categories',
    'category_labels',
    'article_categories'
]

version_names = [
    '3.9',
    '3.8',
    '3.7',
    '3.6',
    '3.5.1',
    '3.5',
    '3.4',
    '3.3',
    '3.2',
    '3.1',
    '3.0',
    '3.0rc',
    '2.0'
]

# The size of the buffer for bz2 decompression
BZ2_BUFFER_SIZE = 10 * 1024

class DBpediaResource(object):
    """
    Class for representing a DBpedia resource.
    Provides a simple api for obtaining an uncompressed file
    stream of the contents of the resource for further processing.
    """
    def __init__(self, dataset, version, language, format="nt"):
        """
        Create a new dbpedia resource.
        :param dataset:
        :param version:
        :param language:
        :param format:
        """
        if dataset not in dataset_names:
            raise Exception("Dataset name %s not recognized" % dataset)
        if version not in version_names:
            raise Exception("Version name %s not recognized" % version)

        self.dataset = dataset
        self.version = version
        self.language = language
        self.format = format

    def get_file(self):
        """
        Downloads the resource if necessary and opens an
        uncompressed stream for reading.
        """
        local_filename = download.retrieve(self)
        return bz2.open(local_filename, mode='rt')

    def clean(self):
        """
        removes any local cached file for this resource
        """
        download.clean(self)

    @staticmethod
    def clean_all():
        """
        removes ALL cached files
        """
        download.clean_all()


def _test():
    import nose.tools as nt

    res = DBpediaResource(dataset="category_labels",
                          version='3.9',
                          language='en',
                          format='nt')

    with res.get_file() as s:
        top = s.read(200)

    nt.assert_equal(len(top), 200)

    lines = top.splitlines()
    nt.assert_equal(len(lines), 3)

    nt.assert_equal(lines[0], '# started 2013-07-10T03:11:48Z')

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)
