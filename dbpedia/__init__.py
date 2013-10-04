
__all__ = ['DBpediaResource', 'NTripleParser']

from resource import DBpediaResource
from ntparser import NTripleParser

import urls, download, resource, ntparser, datasets

if __name__ == "__main__":
    import sys
    import logging

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('dbpedia')

    # empty the cache before we begin
    download.clean_all()

    to_test = [urls, download, resource, ntparser, datasets]

    for module in to_test:
        try:
            log.info("Testing %s" % str(module))
            module._test()

            log.info("Tests Passed")
        except AssertionError as e:
            log.error("ERROR: TESTS FAILED IN %s", str(module))
            log.error(e)
