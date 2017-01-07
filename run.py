#!/usr/bin/env python

import re
import Queue
import os.path
import threading
import urlparse
import requests
import argparse
import logging
import time
import signal
import sys

from contextlib import contextmanager
from urllib import unquote
from operator import methodcaller

logging.basicConfig()
log = logging.getLogger("py-get")
log.setLevel(logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", dest='dry_run', default=False, action='store_true')
parser.add_argument("--debug", dest="debug", default=False, action='store_true')
parser.add_argument("--file", dest='file', default=None)
parser.add_argument("--exclude-pattern", dest='exclude_pattern', default=None)
parser.add_argument("url", nargs="*")

args = parser.parse_args()

if args.debug:
    log.setLevel(logging.DEBUG)

class Stats(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.skipped = 0
        self.completed = 0

    def skip(self):
        with self.update():
            self.skipped += 1

    def complete(self):
        with self.update():
            self.completed += 1

    @contextmanager
    def update(self):
        self.lock.acquire()
        yield
        self.lock.release()

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
    def runloop(kill, queue, stats):
        try:
            while not kill.is_set():
                item = queue.get_nowait()
                local_target = helper.target_from_uri(item)

                if args.dry_run:
                    log.warn("DRY: Would have downloaded %s to %s" % (item, local_target))
                else:
                    helper.make_path(local_target)

                    if not os.path.exists(local_target):
                        log.debug("Retrieving %s", item)
                        aborted = False
                        try:
                            with open(local_target, 'wb') as f:
                                src = requests.get(item, stream=True)
                                for chunk in src.iter_content(1024 * 10):
                                    if kill.is_set():
                                        aborted = True
                                        log.info("Exiting mid-download due to request")
                                        break

                                    f.write(chunk)

                                src.close()
                        except Exception, e:
                            log.warn("Failed downloading {}: {}".format(item, e))
                            aborted = True

                        if aborted:
                            if os.path.exists(local_target):
                                os.remove(local_target)
                                return

                        stats.complete()
                    else:
                        stats.skip()

        except Queue.Empty:
            log.info("Queue empty, exiting thread")

    @staticmethod
    def statusloop(kill, queue, stats):
        def format(elap):
            elap = int(elap)

            h = elap / 3600
            m = (elap % 3600) / 60
            s = elap % 60

            return "%02dh%02dm%02ds" % (h, m, s)

        def estimate(rem, elap):
            c = stats.completed
            per = c / elap

            ttc = rem * per

            return format(ttc)
            

        ts = time.time()
        while not queue.empty() and not kill.is_set():
            rem = queue.qsize()
            elap = time.time() - ts

            sys.stdout.write("\rS:%s C:%s R:%s (elap %s, rem %s)" % (stats.skipped, stats.completed, rem, format(elap), estimate(rem, elap)))
            sys.stdout.flush()
            time.sleep(1)


class Runner(object):
    def __init__(self, args):
        self.args = args
        pass

    def fetch_leaves(self, uri, leaves=None):
        """
        Recursively traverse down the tree and find all the
        leaf nodes
        """
        leaves = leaves if leaves is not None else set()

        if helper.is_dir(uri):
            log.info("Descending into %s", uri)
            for link in self.get_index(uri):
                self.fetch_leaves(link, leaves)
        else:
            log.debug("Found leaf %s", uri)
            if not self.ignore_leaf(uri):
                leaves.add(uri)
            else:
                log.debug("Ignored %s", uri)

        return leaves

    def ignore_leaf(self, url):
        if self.args.exclude_pattern:
            return re.search(self.args.exclude_pattern, url) 
        else:
            return False

    def get_index(self, uri):
        """
        Gets and parses the index of a dir
        """
        link_pattern = r"href=[\"']{1}(.*?)[\"']{1}"
        try:
            response = requests.get(uri)
        except Exception, e:
            log.warn("Failed to get {}: {}".format(uri, e))
            with open('failed.log', 'wb') as out:
                out.write(uri + "\n")

            return set()

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
        event = threading.Event()
        stats = Stats()
        map(queue.put, links)

        threads = []
        for i in range(5):
            threads.append(threading.Thread(name="thread-%02d"%(i), target=helper.runloop, args=[event, queue, stats]))

        threads.append(threading.Thread(name="status", target=helper.statusloop, args=[event, queue, stats]))

        def killthreads(*args):
            print "Exiting"
            event.set()

        signal.signal(signal.SIGINT, killthreads)

        log.info("Starting threads")
        map(methodcaller('start'), threads)

        while any(map(lambda x: x.isAlive(), threads)):
            map(methodcaller('join', 1), threads)

r = Runner(args)
urls = args.url

if args.file:
    with open(args.file, 'r') as contents:
        urls.extend(contents.read().split("\n"))

for u in urls:
    print u
    links = r.fetch_leaves(u)
    log.info("Found %s links for %s", len(links), u)
    r.fetch_all(links)
