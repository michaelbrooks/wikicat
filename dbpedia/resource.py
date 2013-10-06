#
# Convenient class for representing dbpedia resources.
#

__all__ = ['DBpediaResource']

import bz2
import download

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
        return bz2.BZ2File(local_filename, mode='r', buffering=BZ2_BUFFER_SIZE)

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
