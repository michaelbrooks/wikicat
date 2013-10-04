"""
This file is responsible for downloading dataset files from dbpedia.
They will be locally cached in a temporary directory.
If already present, they will not be re-downloaded.
"""

__all__ = ['retrieve', 'clean', 'clean_all']

import os, time
from string import Template
import logging
log = logging.getLogger("dbpedia.download")

import requests, urls

# directory for cached files
CACHE_DIR = '.dbpedia_cache'
# file download chunk size in bytes
CHUNK_SIZE = 50 * 1024
# template for paths to cached resources
cache_file_template = Template('${version}/${language}/${format}/${dataset}.bz2')

def _sizeof_fmt(num):
    """
    Gets a human-readable file size from a number of bytes.
    http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size

    :param num:
    :return:
    """
    for x in ['bytes','KB','MB','GB','TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def _resource_filename(resource):
    """
    Gets the unique cache location for the dbpedia resource
    specified by dataset, version, language, and format.

    :param resource:
    :return:
    """
    resource = cache_file_template.substitute(dataset=resource.dataset,
                                              language=resource.language,
                                              version=resource.version,
                                              format=resource.format)
    # add on the base cache dir
    resource = os.path.join(CACHE_DIR, resource)

    # make sure the directory exists
    dir = os.path.dirname(resource)
    if not os.path.exists(dir):
        os.makedirs(dir)

    return os.path.abspath(resource)

def _download(remote_name, local_name, chunk_size=CHUNK_SIZE):
    """
    Download a remote file to a local file.
    A chunk size can be provided to set the size of download chunks.

    :param remote_name:
    :param local_name:
    :param chunk_size:
    :return:
    """

    log.info("Downloading from %s", remote_name)

    before = time.time()

    # http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    req = requests.get(remote_name, stream = True) # here we need to set stream = True parameter

    with open(local_name, 'wb') as out_file:

        for chunk in req.iter_content(chunk_size=chunk_size):

            if chunk: # filter out keep-alive new chunks

                out_file.write(chunk)
                out_file.flush()

    after = time.time()

    bytes = os.path.getsize(local_name)
    size = _sizeof_fmt(bytes)
    duration = after - before
    rate = _sizeof_fmt(bytes / duration)

    log.info("Downloaded %s in %fs (%s/s)", size, duration, rate)

    return local_name


def retrieve(resource):
    """
    Returns a path to a dbpedia file, stored locally.
    The file will be available for reading.
    This function may block for some time while downloading the file,
    or return quickly if the file was cached.

    If, when reading the file, there is an IOError, it may
    mean that the file download was incomplete. In that case,
    the clean() function should be used to delete the cached file.
    :param resource:
    :return:
    """

    # Generate the local filename for this resource (creating directories as needed)
    local_name = _resource_filename(resource)

    # see if it exists
    log.info("Checking cache for %s", local_name)
    if not os.path.exists(local_name):

        # if not, go ahead and download it
        remote_name = urls.build(resource)

        _download(remote_name, local_name)
    else:
        log.info("Using cached file %s", local_name)

    return local_name

def clean(resource):
    """
    Remove the cached version of a particular resource

    :param resource:
    :return:
    """

    # Generate the local filename for this resource
    local_name = _resource_filename(resource)

    os.remove(local_name)

    log.info("Cleaned cache for %s", local_name)

def clean_all():
    """
    Remove all cached files
    :return:
    """

    if os.path.exists(CACHE_DIR):
        import shutil
        shutil.rmtree(CACHE_DIR)


def _test():
    import nose.tools as nt
    import time
    from resource import DBpediaResource

    # delete the cache
    clean_all()

    # Try a small resource
    r = DBpediaResource(dataset="category_labels",
                        version='3.9',
                        language='en',
                        format='nt')

    # get it
    before = time.time()
    local_name = retrieve(r)
    after = time.time()

    # that should have taken some time
    nt.ok_(after - before > 1) # more than 1 second

    # Just a sanity check
    nt.assert_equal(local_name, _resource_filename(r))

    # we already know how big this one file is supposed to be
    nt.ok_(os.path.getsize(local_name) > 11200000)

    # try it again
    before = time.time()
    local_name = retrieve(r)
    after = time.time()

    # this time it should be near instant
    nt.ok_(after - before < 0.1)

    # now delete it
    clean(r)
    nt.ok_(not os.path.exists(local_name))

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        _test()
        logging.info("Tests Passed")
    except AssertionError as e:
        logging.error("ERROR: TESTS FAILED")
        logging.error(e)
