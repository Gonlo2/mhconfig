import time
import traceback
from collections import namedtuple
from dataclasses import dataclass
from enum import IntEnum
from queue import Queue
from threading import Event, Lock, Thread
from typing import Any, List, Optional, Tuple

import grpc
from frozendict import frozendict
from mhconfig.proto import mhconfig_pb2

NamespaceKey = namedtuple('NamespaceKey', ['root_path', 'overrides'])
VersionKey = namedtuple('VersionKey', ['namespace_id', 'version'])
ConfigKey = namedtuple('ConfigKey', ['document', 'flavors'])

SpecificConfigKey = namedtuple('SpecificConfigKey', ['version_key', 'config_key'])


@dataclass
class WatchCommand:
    uid: int
    namespace_key: Optional[NamespaceKey] = None
    remove: bool = False
    config_key: ConfigKey = None


class Touchable:
    def __init__(self):
        self._last_touch = 0

    def last_touch(self) -> int:
        return self._last_touch

    def touch(self):
        self._last_touch = time.time()


class SpecificConfig(Touchable):
    def __init__(self, value: Any, checksum: bytes):
        super().__init__()
        self._value = value
        self._checksum = checksum

    def value(self) -> Any:
        return self._value

    def checksum(self) -> bytes:
        return self._checksum

    def __repr__(self) -> str:
        return 'SpecificConfig(value: {}, checksum: {})'.format(
            self._value,
            self._checksum,
        )


class Config(Touchable):
    def __init__(self, key: ConfigKey, uid: int):
        super().__init__()
        self._key = key
        self._uid = uid
        self._status = None
        self._version_key = None
        self._specific_config = None
        self._next_callback_uid = 0
        self._callback_by_uid = {}
        self._active = True
        self._lock = Lock()

    def key(self) -> ConfigKey:
        return self._key

    def uid(self) -> int:
        return self._uid

    def is_active(self) -> bool:
        return self._active

    def deactive(self) -> bool:
        with self._lock:
            if self._active:
                self._active = False
                self._status = mhconfig_pb2.WatchResponse.Status.REMOVED
                self._trigger_callbacks()
                return True
        return False

    def can_deactive(self) -> bool:
        with self._lock:
            return not self._callback_by_uid and self._active

    def latest(self) -> Tuple[VersionKey, SpecificConfig]:
        with self._lock:
            return (self._version_key, self._specific_config)

    def update(self, status, version_key, specific_config):
        with self._lock:
            if self._active and self._need_update(version_key):
                self._version_key = version_key
                if self._status != status \
                        or self._specific_config.checksum() != specific_config.checksum() \
                        or self._specific_config is None:
                    self._status = status
                    self._specific_config = specific_config
                    self._trigger_callbacks()

    def _trigger_callbacks(self):
        for c in self._callback_by_uid.values():
            c(self._status, self._version_key, self._specific_config)

    def _need_update(self, version_key) -> bool:
        return self._version_key is None \
                or self._version_key.namespace_id != version_key.namespace_id \
                or self._version_key.version < version_key.version

    def add_callback(self, callback):
        with self._lock:
            if not self._active:
                return None
            uid = self._next_callback_uid
            self._next_callback_uid += 1
            self._callback_by_uid[uid] = callback
            if self._version_key is not None:
                callback(self._status, self._version_key, self._specific_config)
            return uid

    def remove_callback(self, uid: int):
        with self._lock:
            if self._active:
                return self._callback_by_uid.pop(uid, None) is not None
        return False


def _cleanup_specific_config(timelimit, values, limit):
    if len(values) > limit:
        last_touch_and_key = [(sc.last_touch(), k) for k, sc in values.items()]
        last_touch_and_key.sort(lambda x: x[0])
        remove_till = max(len(values) - ((limit*3)//4), 0)
        for _, k in last_touch_and_key[remove_till:]:
            del values[k]


class UndefinedElement(object):
    def __eq__(self, other):
        return isinstance(other, UndefinedElement)

    def __ne__(self, other):
        return not isinstance(other, UndefinedElement)


class ServerChangedException(Exception):
    pass


class ValueElement(IntEnum):
    STR = 0
    UNDEFINED = 1
    INT = 2
    DOUBLE = 3
    BOOL = 4
    NULL = 5
    MAP = 6
    SEQUENCE = 7
    BIN = 8


class KeyElement(IntEnum):
    STR = 0


def decode(elements: List[mhconfig_pb2.Element]) -> Any:
    return _decode(elements, 0) if elements else UndefinedElement()


def _decode(elements: List[mhconfig_pb2.Element], idx: int) -> Any:
    element = elements[idx]
    value_element = ValueElement(element.type & 15)
    if value_element == ValueElement.STR:
        return element.value_str
    elif value_element == ValueElement.BIN:
        return element.value_bin
    elif value_element == ValueElement.INT:
        return element.value_int
    elif value_element == ValueElement.DOUBLE:
        return element.value_double
    elif value_element == ValueElement.BOOL:
        return element.value_bool
    elif value_element == ValueElement.MAP:
        result = {}
        for _ in range(element.size):
            idx += 1
            key_element = KeyElement((elements[idx].type>>4) & 7)
            if key_element == KeyElement.STR:
                result[elements[idx].key_str] = _decode(elements, idx)
            else:
                raise NotImplementedError
            idx += elements[idx].sibling_offset
        return frozendict(result)
    elif value_element == ValueElement.SEQUENCE:
        result = []
        for _ in range(element.size):
            idx += 1
            result.append(_decode(elements, idx))
            idx += elements[idx].sibling_offset
        return tuple(result)
    elif value_element == ValueElement.NULL:
        return None
    elif value_element == ValueElement.UNDEFINED:
        return UndefinedElement()

    raise NotImplementedError


class NamespaceContext:
    def __init__(self, key: NamespaceKey):
        super().__init__()
        self._key = key
        self._config_by_key = {}
        self._specific_config_by_key = {}

    def key(self) -> NamespaceKey:
        return self._key

    def cleanup(self, timelimit):
        to_remove = []
        for k, config in self._config_by_key.items():
            if config.last_touch() < timelimit and config.can_deactive():
                config.deactive()
                to_remove.append(k)
        for k in to_remove:
            del self._config_by_key[k]

        _cleanup_specific_config(timelimit, self._specific_config_by_key, 1000)

    def get_config(self, key: ConfigKey) -> Optional[Config]:
        return self._config_by_key.get(key)

    def set_config(self, key: ConfigKey, config: Config):
        self._config_by_key[key] = config

    def get_specific_config(self, key: ConfigKey, version_key: VersionKey = None):
        if version_key is None:
            config = self._config_by_key.get(key)
            if config is None:
                return (None, None)
            config.touch()
            return config.latest()

        specific_config_key = SpecificConfigKey(
            version_key=version_key,
            config_key=key,
        )
        return (version_key, self._specific_config_by_key.get(specific_config_key))

    def set_specific_config(self, specific_config_key: SpecificConfigKey, specific_config: SpecificConfig):
        self._specific_config_by_key[specific_config_key] = specific_config


class Context:
    def __init__(self):
        super().__init__()
        self._ns_and_config_by_uid = {}
        self._specific_config_by_checksum = {}
        self._next_watcher_uid = 0
        self._namespace_ctx_by_key = {}

    def reset(self):
        #TODO Recconect the watchers
        for _, v in self._ns_and_config_by_uid.values():
            v.deactive()
        self._ns_and_config_by_uid.clear()
        self._namespace_ctx_by_key.clear()
        self._next_watcher_uid = 0

    def cleanup(self, timelimit):
        to_remove = []
        for uid, (_, config) in self._ns_and_config_by_uid.items():
            if config.last_touch() < timelimit and config.can_deactive():
                config.deactive()
                to_remove.append(uid)
        for uid in to_remove:
            del self._ns_and_config_by_uid[uid]

        _cleanup_specific_config(timelimit, self._specific_config_by_checksum, 1000)

        for ns_ctx in self._namespace_ctx_by_key.values():
            ns_ctx.cleanup(timelimit)

        return to_remove

    def get_namespace_ctx(self, key: NamespaceKey):
        ns_ctx = self._namespace_ctx_by_key.get(key)
        if ns_ctx is None:
            ns_ctx = NamespaceContext(key)
            self._namespace_ctx_by_key[key] = ns_ctx
        return ns_ctx

    def get_or_make_config(self, ns_ctx: NamespaceContext, key: ConfigKey):
        config = ns_ctx.get_config(key)
        is_new = config is None
        if is_new:
            uid = self._next_watcher_uid
            self._next_watcher_uid += 1
            config = Config(key, uid)
            self._ns_and_config_by_uid[uid] = (ns_ctx, config)
            ns_ctx.set_config(key, config)
        return (is_new, config)

    def get_watcher_ns_and_config_config(self, uid: int):
        return self._ns_and_config_by_uid.get(uid)

    def pop_watcher(self, uid: int):
        return self._ns_and_config_by_uid.pop(uid)

    def get_specific_config(self, specific_config_key, r):
        specific_config = self._specific_config_by_checksum.get(r.checksum)
        if specific_config is None:
            value = decode(r.elements)
            specific_config = SpecificConfig(value, r.checksum)
            self._specific_config_by_checksum[r.checksum] = specific_config
        return specific_config


class Client:
    def __init__(
            self,
            stub,
            metadata: Tuple[Tuple[str, str]] = None,
            cleanup_period_sec: int = 60,
            cleanup_inactivity_sec: int = 60,
    ):
        super().__init__()
        self._stub = stub
        self._metadata = metadata
        self._cleanup_period_sec = cleanup_period_sec
        self._cleanup_inactivity_sec = cleanup_inactivity_sec

        self._lock = Lock()
        self._ctx = Context()
        self._close = False

        self._watch_queue = Queue()
        self._watch_it = None
        self._watch_thread = None

        self._cleanup_thread = None
        self._cleanup_event = Event()

    def init(self):
        self._watch_thread = Thread(target=self._watch_loop)
        self._watch_thread.start()

        self._cleanup_thread = Thread(target=self._cleanup_loop)
        self._cleanup_thread.start()

    def close(self):
        self._close = True
        self._cleanup_event.set()
        self._watch_queue.put((True, None))

        self._cleanup_thread.join()
        self._watch_thread.join()

        self._watch_it.cancel()

    def _watch_loop(self):
        while True:
            try:
                self._watch_it = self._stub.Watch(
                    self._watch_input(),
                    metadata=self._metadata,
                )
                for r in self._watch_it:
                    with self._lock:
                        ns_and_config = self._ctx.get_watcher_ns_and_config_config(r.uid)
                    if ns_and_config is not None:
                        ns_and_config[1].touch()
                        self._on_watch_reply(r, ns_and_config[0], ns_and_config[1])
            except grpc._channel._Rendezvous as err:
                if err.cancelled() and self._close:
                    break
                traceback.print_exc()  # TODO
            except Exception:
                traceback.print_exc()  # TODO
                raise

            with self._lock:
                self._ctx.reset()

            time.sleep(1)

    def _on_watch_reply(self, r, ns_ctx, config):
        if r.status == mhconfig_pb2.WatchResponse.Status.REMOVED:
            if config.is_active():
                #TODO Add throttling
                command = WatchCommand(
                    uid=r.uid,
                    document=config.document,
                )
                self._watch_queue.put((False, command))
            else:
                with self._lock:
                    self._ctx.pop_watcher(r.uid)
        else:
            version_key = VersionKey(
                namespace_id=r.namespace_id,
                version=r.version,
            )

            specific_config_key = SpecificConfigKey(
                version_key=version_key,
                config_key=config.key(),
            )

            specific_config = self._ctx.get_specific_config(specific_config_key, r)
            ns_ctx.set_specific_config(specific_config_key, specific_config)
            ns_ctx.set_config(config.key(), config)
            specific_config.touch()
            config.update(r.status, version_key, specific_config)

    def _watch_input(self):
        while True:
            try:
                exit, command = self._watch_queue.get()
                if exit:
                    break

                if command.remove:
                    yield mhconfig_pb2.WatchRequest(
                        uid=command.uid,
                        remove=True,
                    )
                else:
                    yield mhconfig_pb2.WatchRequest(
                        uid=command.uid,
                        root_path=command.namespace_key.root_path,
                        overrides=command.namespace_key.overrides,
                        document=command.config_key.document,
                        flavors=command.config_key.flavors,
                    )
            except Exception:
                traceback.print_exc()
                raise

    def _cleanup_loop(self):
        while True:
            self._cleanup_event.wait(self._cleanup_period_sec)
            if self._close:
                break
            timelimit = time.time() - self._cleanup_inactivity_sec
            with self._lock:
                for uid in self._ctx.cleanup(timelimit):
                    command = WatchCommand(uid=uid, remove=True)
                    self._watch_queue.put((False, command))

    def watch(self, namespace_key: NamespaceKey, config_key: ConfigKey, callback=None):
        with self._lock:
            ns_ctx = self._ctx.get_namespace_ctx(namespace_key)
            is_new, config = self._ctx.get_or_make_config(ns_ctx, config_key)
            if is_new:
                command = WatchCommand(
                    uid=config.uid(),
                    namespace_key=ns_ctx.key(),
                    config_key=config_key,
                )
                self._watch_queue.put((False, command))
        config.touch()
        callback_uid = None if callback is None else config.add_callback(callback)
        return (config.uid(), callback_uid)

    def unwatch(self, watcher_uid: int, callback_uid: int):
        with self._lock:
            ns_and_config = self._ctx.get_watcher_ns_and_config_config(watcher_uid)
            if ns_and_config is None:
                return False
            return ns_and_config[1].remove_callback(callback_uid)

    def get(
            self,
            namespace_key: NamespaceKey,
            config_key: ConfigKey,
            version_key: VersionKey = None,
    ):
        with self._lock:
            ns_ctx = self._ctx.get_namespace_ctx(namespace_key)
            version_key, specific_config = ns_ctx.get_specific_config(
                config_key,
                version_key=version_key,
            )
            if specific_config is not None:
                specific_config.touch()
                return (
                    mhconfig_pb2.GetResponse.Status.OK,
                    version_key,
                    specific_config,
                )

            request = mhconfig_pb2.GetRequest(
                root_path=namespace_key.root_path,
                overrides=namespace_key.overrides,
                document=config_key.document,
                flavors=config_key.flavors or tuple(),
                version=0 if version_key is None else version_key.version,
            )
            r = self._stub.Get(request, metadata=self._metadata)

            new_version_key = VersionKey(
                namespace_id=r.namespace_id,
                version=r.version,
            )

            if version_key is not None and version_key != new_version_key:
                raise ServerChangedException

            specific_config_key = SpecificConfigKey(
                version_key=version_key or new_version_key,
                config_key=config_key,
            )

            specific_config = self._ctx.get_specific_config(specific_config_key, r)
            ns_ctx.set_specific_config(specific_config_key, specific_config)
            specific_config.touch()
            return (r.status, new_version_key, specific_config)


class NamespaceClient:
    def __init__(
            self,
            client: Client,
            namespace_key: NamespaceKey,
    ):
        super().__init__()
        self._client = client
        self._namespace_key = namespace_key

    def watch(self, config_key: ConfigKey, callback=None):
        return self._client.watch(
            self._namespace_key,
            config_key,
            callback=callback
        )

    def unwatch(self, watcher_uid: int, callback_uid: int):
        return self._client.unwatch(watcher_uid, callback_uid)

    def get(self, config_key: ConfigKey, version_key: VersionKey = None):
        return self._client.get(
            self._namespace_key,
            config_key,
            version_key=version_key
        )


class Session:
    def __init__(self, client: NamespaceClient):
        self._client = client
        self._version_key = None
        self._configs = {}
        self._lock = Lock()

    def get(self, key: ConfigKey):
        with self._lock:
            specific_config = self._configs.get(key)
            if specific_config is None:
                status, version_key, specific_config = self._client.get(
                    key,
                    version_key=self._version_key
                )
                if self._version_key is None:
                    self._version_key = version_key
                if status != mhconfig_pb2.GetResponse.Status.OK:
                    return (status, specific_config)
                self._client.watch(key)
                self._configs[key] = specific_config
            return (mhconfig_pb2.GetResponse.Status.OK, specific_config)


def make_session(client: NamespaceClient):
    return Session(client)
