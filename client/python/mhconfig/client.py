import time
import traceback
from queue import Queue
from threading import Lock, Thread, Event
from collections import defaultdict

from frozendict import frozendict

import grpc

from mhconfig.proto import mhconfig_pb2, mhconfig_pb2_grpc


class Future(object):
    def __init__(self):
        super(Future, self).__init__()
        self._event = Event()
        self._value = None
        self._exception = None

    def _set_exception(self, exception):
        self._exception = exception
        self._event.set()

    def _set(self, value):
        self._value = value
        self._event.set()

    def get(self, timeout=None):
        self._event.wait(timeout=timeout)

        if self._exception is not None:
            raise self._exception

        return self._value


class UndefinedElement(object):
    pass


class ServerChangedException(Exception):
    pass


def _decode(elements, idx):
    element = elements[idx]
    if element.type == mhconfig_pb2.Element.NodeType.SCALAR_NODE:
        return element.value
    elif element.type == mhconfig_pb2.Element.NodeType.MAP_NODE:
        result = {}
        for _ in range(element.size):
            idx += 1
            result[elements[idx].key] = _decode(elements, idx)
            idx += elements[idx].sibling_offset
        return frozendict(result)
    elif element.type == mhconfig_pb2.Element.NodeType.SEQUENCE_NODE:
        result = []
        for _ in range(element.size):
            idx += 1
            result.append(_decode(elements, idx))
            idx += elements[idx].sibling_offset
        return tuple(result)
    elif element.type == mhconfig_pb2.Element.NodeType.NULL_NODE:
        return None
    elif element.type == mhconfig_pb2.Element.NodeType.UNDEFINED_NODE:
        return UndefinedElement
    else:
        raise NotImplementedError


class Client(object):
    def __init__(self, address, root_path, overrides, cleanup_period_sec=60, cleanup_inactivity_sec=60):
        super(Client, self).__init__()
        self._address = address
        self._root_path = root_path
        self._overrides = overrides
        self._cleanup_period_sec = cleanup_period_sec
        self._cleanup_inactivity_sec = cleanup_inactivity_sec

        self._lock = Lock()
        self._configs = {}
        self._latest_configs = {}
        self._close = False

        self._watchers = defaultdict(list)
        self._watch_document_by_uid = {}
        self._watch_queue = Queue()
        self._next_watcher_uid = 0

        self._watch_thread = None
        self._watch_it = None

        self._cleanup_thread = None
        self._cleanup_event = Event()

    def init(self):
        self._channel = grpc.insecure_channel(self._address)
        self._stub = mhconfig_pb2_grpc.MHConfigStub(self._channel)

        self._watch_thread = Thread(target=self._watch_loop)
        self._watch_thread.start()

        self._cleanup_thread = Thread(target=self._cleanup_loop)
        self._cleanup_thread.start()

    def close(self):
        self._close = True
        self._cleanup_event.set()
        self._watch_queue.put((True, None))
        self._watch_it.cancel()

        self._cleanup_thread.join()
        self._watch_thread.join()

        self._channel.close()
        with self._lock:
            for w in self._watchers.values():
                w._set_exception(Exception)

    def new_session(self):
        return Session(self)

    def _watch_input_messages(self):
        while True:
            try:
                exit, payload = self._watch_queue.get()
                if exit:
                    break

                uid, remove, version, document = payload
                if remove:
                    yield mhconfig_pb2.WatchRequest(
                        uid=uid,
                        remove=True,
                    )
                else:
                    yield mhconfig_pb2.WatchRequest(
                        uid=uid,
                        root_path=self._root_path,
                        overrides=self._overrides,
                        version=version,
                        document=document,
                    )
            except:
                traceback.print_exc()
                raise

    def _watch_loop(self):
        while True:
            with self._lock:
                self._watchers.clear()
                self._watch_document_by_uid.clear()
                self._next_watcher_uid = 0
                self._latest_configs.clear()
            try:
                self._watch_it = self._stub.Watch(self._watch_input_messages())
                for r in self._watch_it:
                    elements = _decode(r.elements, 0) \
                        if r.elements \
                        else UndefinedElement
                    with self._lock:
                        self._on_watch_response(r, elements)
            except grpc._channel._Rendezvous as err:
                if err.cancelled() and self._close:
                    break
                traceback.print_exc()  # TODO
            except:
                traceback.print_exc()  # TODO
                raise
            time.sleep(1)

    def _on_watch_response(self, r, elements):
        document = self._watch_document_by_uid.get(r.uid)
        if r.status == mhconfig_pb2.WatchResponse.Status.REMOVED:
            if document is not None:
                self._watch(r.uid, 0, document)
        else:
            if r.status == mhconfig_pb2.WatchResponse.Status.OK:
                namespace_id_and_version = (r.namespace_id, r.version)

                key = (namespace_id_and_version, document)

                t = time.time()
                self._latest_configs[document] = [
                    t,
                    r.uid,
                    (namespace_id_and_version, elements),
                ]
                self._configs[key] = [t, elements]

            watchers = self._watchers.pop(document, [])
            value = (r.status, r.namespace_id, r.version, elements)
            while watchers:
                watchers.pop()._set(value)

    def _cleanup_loop(self):
        while True:
            self._cleanup_event.wait(self._cleanup_period_sec)
            if self._close:
                break
            t = time.time() - self._cleanup_inactivity_sec
            with self._lock:
                latest_config_to_remove = [
                    (k, uid) for k, (tt, uid, _) in self._latest_configs.items()
                    if tt < t
                ]
                for k, uid in latest_config_to_remove:
                    self._watch_document_by_uid.pop(uid)
                    self._unwatch(uid)
                    del self._latest_configs[k]
                self._configs = {
                    k: v for k, v in self._configs.items() if t <= v[0]
                }

    def get(self, document, namespace_id_and_version=None):
        if namespace_id_and_version is None:
            time_and_result = self._latest_configs.get(document)
            if time_and_result is not None:
                time_and_result[0] = time.time()
                return (mhconfig_pb2.GetResponse.Status.OK, time_and_result[2])

            f = Future()
            with self._lock:
                self._watchers[document].append(f)
                uid = self._next_watcher_uid
                self._watch_document_by_uid[uid] = document
                self._next_watcher_uid += 1
            self._watch(uid, 0, document)
            status, namespace_id, version, elements = f.get()
            return (status, ((namespace_id, version), elements))
        else:
            key = (namespace_id_and_version, document)
            time_and_elements = self._configs.get(key)
            if time_and_elements is None:
                with self._lock:
                    time_and_elements = self._configs.get(key)
                    if self._latest_configs.get(document) is None:
                        uid = self._next_watcher_uid
                        self._watch_document_by_uid[uid] = document
                        self._next_watcher_uid += 1
                        self._watch(uid, namespace_id_and_version[1], document)
            if time_and_elements is not None:
                time_and_elements[0] = time.time()
                return (
                    mhconfig_pb2.GetResponse.Status.OK,
                    (namespace_id_and_version, time_and_elements[1]),
                )

            status, namespace_id, version, elements = self._get(
                document,
                version=namespace_id_and_version[1],
            )
            if (namespace_id, version) != namespace_id_and_version:
                raise ServerChangedException
            if status == mhconfig_pb2.GetResponse.Status.OK:
                key = ((namespace_id, version), document)
                self._configs[key] = [time.time(), elements]
            return (status, ((namespace_id, version), elements))

    def _watch(self, uid, version, document):
        self._watch_queue.put((False, (uid, False, version, document)))

    def _unwatch(self, uid):
        self._watch_queue.put((False, (uid, True, None, None)))

    def _get(self, document, version=None, key=None):
        request = mhconfig_pb2.GetRequest(
            root_path=self._root_path,
            overrides=self._overrides,
            document=document,
            key=key or [],
            version=version or 0,
        )
        response = self._stub.Get(request)
        elements = _decode(response.elements, 0) \
            if response.elements \
            else UndefinedElement
        return (response.status, response.namespace_id, response.version, elements)


class Session(object):
    def __init__(self, mhconfig):
        super(Session, self).__init__()
        self._mhconfig = mhconfig
        self._namespace_id_and_version = None
        self._configs = {}

    def get(self, document):
        config = self._configs.get(document)
        if config is not None:
            return (mhconfig_pb2.WatchResponse.Status.OK, config)

        status, (self._namespace_id_and_version, config) = self._mhconfig.get(
            document,
            namespace_id_and_version=self._namespace_id_and_version,
        )
        if status == mhconfig_pb2.WatchResponse.Status.OK:
            self._configs[document] = config
        return (status, config)
