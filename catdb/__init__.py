import models, mysql

if __name__ == "__main__":
    import sys
    import logging

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('catdb')

    # empty the cache before we begin
    to_test = [models, mysql]

    for module in to_test:
        try:
            log.info("Testing %s" % str(module))
            module._test()

            log.info("Tests Passed")
        except AssertionError as e:
            log.error("ERROR: TESTS FAILED IN %s", str(module))
            log.error(e)
