#!/usr/bin/env python

import re
import Queue
import os.path
import threading
import urlparse
import requests
import argparse
import logging

from urllib import unquote
from operator import methodcaller

logging.basicConfig()
log = logging.getLogger("py-get")
log.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", dest='dry_run', default=False, action='store_true')
parser.add_argument("url", nargs="*")

args = parser.parse_args()

class helper(object):
    lock = threading.RLock()

    @staticmethod
    def is_dir(uri):
        return uri.endswith("/")

    @staticmethod
    def is_parent(uri, test_uri):
        if len(test_uri) < len(uri) and test_uri in uri:
            return True
        else:
            return False

    @staticmethod
    def target_from_uri(uri):
        """
        Expands a URI into a local filepath
        """
        return unquote(re.sub(r"^http.*?://", "", uri))

    @staticmethod
    def make_path(local_path):
        """
        Makes a path so the file can exist
        """
        target = os.path.dirname(local_path)

        with helper.lock:
            if not os.path.exists(target):
                os.makedirs(target)
        
    @staticmethod
    def runloop(queue):
        try:
            while True:
                item = queue.get_nowait()
                local_target = helper.target_from_uri(item)

                if args.dry_run:
                    log.warn("DRY: Would have downloaded %s to %s" % (item, local_target))
                else:
                    helper.make_path(local_target)

                    if not os.path.exists(local_target):
                        log.info("Retrieving %s", item)
                        with open(local_target, 'wb') as f:
                            src = requests.get(item, stream=True)
                            for chunk in src.iter_content(1024 * 10):
                                f.write(chunk)
                            src.close()

        except Queue.Empty:
            log.info("Queue empty, exiting thread")

class Runner(object):
    def __init__(self):
        pass

    def fetch_leaves(self, uri, leaves=None):
        """
        Recursively traverse down the tree and find all the
        leaf nodes
        """
        leaves = leaves if leaves is not None else set()

        if helper.is_dir(uri):
            log.debug("Descending into %s", uri)
            for link in self.get_index(uri):
                self.fetch_leaves(link, leaves)
        else:
            log.debug("Found leaf %s", uri)
            leaves.add(uri)

        return leaves

    def get_index(self, uri):
        """
        Gets and parses the index of a dir
        """
        link_pattern = r"href=[\"']{1}(.*?)[\"']{1}"
        response = requests.get(uri)

        links = set()
        for link in re.finditer(link_pattern, response.text, re.I):
            absolute = urlparse.urljoin(uri, link.groups()[0])
            if helper.is_parent(uri, absolute):
                log.debug("Ignoring %s", absolute)
            else:
                links.add(absolute)

        return links

    def fetch_all(self, links):
        """
        Fetch all the supplied links
        """
        queue = Queue.Queue()
        map(queue.put, links)

        threads = []
        for i in range(5):
            threads.append(threading.Thread(name="thread-%02d"%(i), target=helper.runloop, args=[queue]))

        log.info("Starting threads")
        map(methodcaller('start'), threads)
        map(methodcaller('join'), threads)

r = Runner()

for u in args.url:
    print u
    links = r.fetch_leaves(u)
    log.info("Found %s links", len(links))
    r.fetch_all(links)
