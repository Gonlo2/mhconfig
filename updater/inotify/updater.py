#!/usr/bin/env python3
import sys
import time
import os
import logging
import errno
from pprint import pprint

from inotify_simple import INotify, flags
import grpc

import mhconfig_pb2, mhconfig_pb2_grpc


_FILE_FLAGS = flags.CLOSE_WRITE | flags.MOVED_FROM | flags.MOVED_TO | flags.DELETE
_DIR_FLAGS = flags.CREATE | flags.MOVED_FROM | flags.MOVED_TO | flags.DELETE


_WATCH_FILE_EXTENSION = '.yaml'


logger = logging.getLogger(__name__)



class WatchMetadata(object):
    def __init__(self, path):
        super(WatchMetadata, self).__init__()
        self.path = path
        self._children = {}

    def add_child(self, wd, name):
        self._children[name] = wd

    def get_child_wd(self, name):
        return self._children.get(name)

    def children_wd(self):
        return self._children.values()

    def __repr__(self):
        return 'WatchMetadata(path: {}, children: {})'.format(
            self.path,
            self._children,
        )


class InotifyUpdater(object):
    def __init__(self, address, root_path, batch_time_sec=1):
        super(InotifyUpdater, self).__init__()
        self._address = address
        self._root_path = os.path.normpath(os.path.abspath(root_path))
        self._batch_time_sec = batch_time_sec

        self._inotify = INotify()
        self._watch_metadata_by_wd = {}

    def start(self):
        logger.info("Started inotify updater in '%s'", self._root_path)
        relative_paths_it = self._relative_paths_batch_gen()
        with grpc.insecure_channel(self._address) as channel:
            stub = mhconfig_pb2_grpc.MHConfigStub(channel)
            for relative_paths in relative_paths_it:
                request = mhconfig_pb2.UpdateRequest(
                    root_path=self._root_path,
                    relative_paths=relative_paths,
                )
                response = stub.Update(request)  #TODO check response
                pprint(response)

    def _relative_paths_batch_gen(self):
        while True:
            self._watch_metadata_by_wd.pop(None, None)
            for wd in self._watch_metadata_by_wd.keys():
                self._inotify.rm_watch(wd)

            #TODO do a softdelete of the namespace or reload it
            # (mainly review the mhconfig API)
            for relative_paths in self._unsafe_relative_paths_batch_gen():
                yield relative_paths

    #PS: This dont support move/delete the root path
    def _unsafe_relative_paths_batch_gen(self):
        self._watch_metadata_by_wd = {None: WatchMetadata(self._root_path)}
        self._add_recursive_watch(None, self._root_path, None)
        while True:
            relative_paths = set()

            while True:
                for event in self._inotify.read():
                    if event.mask & flags.IGNORED:
                        continue
                    if event.mask & flags.Q_OVERFLOW:
                        return
                    relative_paths.update(self._process_single_event(event))
                if relative_paths:
                    break

            logger.info("Starting batch update")
            limit_time = time.time() + self._batch_time_sec
            while time.time() < limit_time:
                for event in self._inotify.read(timeout=2):
                    if event.mask & flags.IGNORED:
                        continue
                    if event.mask & flags.Q_OVERFLOW:
                        return
                    relative_paths.update(self._process_single_event(event))

            logger.info("Ended batch update with %s changes", len(relative_paths))
            for x in relative_paths:
                logger.debug("To notify about relative path '%s'", x)

            yield relative_paths

    def _add_recursive_watch(self, name, path, parent_wd):
        relative_paths = []
        ok, wd = self._add_watch(name, path, parent_wd, is_file=False)
        if not ok:
            return relative_paths

        for name in os.listdir(path):
            p = os.path.join(path, name)
            if not name.startswith('.'):
                if os.path.isdir(p):
                    relative_paths.extend(self._add_recursive_watch(name, p, wd))
                elif name.startswith('_') or name.endswith(_WATCH_FILE_EXTENSION):
                    self._add_watch(name, p, wd)
                    relative_paths.append(os.path.relpath(p, self._root_path))
                else:
                    logger.debug("Ignoring the file '%s' because the extension", p)
            else:
                logger.debug("Ignoring '%s' because it's hidden", p)
        return relative_paths

    def _add_watch(self, name, path, parent_wd, is_file=True):
        logger.debug(
            "Watching the %s '%s' in '%s'",
            "file" if is_file else "dir",
            name,
            path,
        )
        try:
            wd = self._inotify.add_watch(path, _FILE_FLAGS if is_file else _DIR_FLAGS)
        except OSError as e:
            logger.exception("Some exception take place calling inotify add_watch")
            if e.errno == errno.ENOENT:
                return (False, None)
            raise e
        self._watch_metadata_by_wd[parent_wd].add_child(wd, name)
        self._watch_metadata_by_wd[wd] = WatchMetadata(path)
        return (True, wd)

    def _process_single_event(self, event):
        logger.debug("Received raw event %s", str(event))
        path = os.path.join(
            self._watch_metadata_by_wd[event.wd].path,
            event.name
        )
        result = []

        if path.startswith(self._root_path) and not event.name.startswith('.'):
            if event.mask & flags.ISDIR:
                if event.mask & (flags.CREATE | flags.MOVED_TO):
                    result = self._add_recursive_watch(event.name, path, event.wd)
            elif event.name.startswith('_') or event.name.endswith(_WATCH_FILE_EXTENSION):
                if event.mask & flags.MOVED_TO:
                    self._add_watch(event.name, path, event.wd)
                result = [os.path.relpath(path, self._root_path)]
            else:
                logger.debug("Ignoring the file '%s' because the extension", path)
        else:
            logger.debug("Ignoring '%s' because it's out the path / hidden", path)

        #TODO handle better the moves
        #PS: https://lwn.net/Articles/605128/
        #PS2: http://man7.org/tlpi/code/online/dist/inotify/inotify_dtree.c.html#processNextInotifyEvent
        if event.mask & (flags.MOVED_FROM | flags.DELETE):
            wd = self._watch_metadata_by_wd[event.wd].get_child_wd(event.name)
            if wd is not None:
                self._rm_watch_tree(wd)

        return result

    def _rm_watch_tree(self, wd):
        watch_metadata = self._watch_metadata_by_wd.pop(wd, None)
        if watch_metadata is not None:
            logger.debug("Removing %s", str(watch_metadata))
            for child_wd in watch_metadata.children_wd():
                self._rm_watch_tree(child_wd)


def main(address, root_path, batch_time_sec):
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=log_level)

    batch_time_sec = float(batch_time_sec)

    updater = InotifyUpdater(address, root_path, batch_time_sec)
    updater.start()


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage: {} <server address> <path to watch> <seconds of grace before trigger the update>".format(sys.argv[0]))
        print("Example: LOG_LEVEL=debug {} 127.0.0.1:2222 ../../examples/config/ 1".format(sys.argv[0]))
        sys.exit(1)

    main(*args)
