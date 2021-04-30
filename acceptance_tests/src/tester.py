#!/usr/bin/env python3
import logging
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import grpc
from mhconfig.client import Client, TraceStatus, WatchStatus
from mhconfig.proto import mhconfig_pb2, mhconfig_pb2_grpc

logger = logging.getLogger(__name__)


@dataclass
class WatchResponse:
    status: mhconfig_pb2.WatchResponse.Status
    namespace_id: int
    version: int
    value: Any
    checksum: bytes


_CONFIG_BASE_RESULT = {
    'another': 'world',
    'hello': {
        'bye': 'bye',
        'seq': [None],
        'dog': 'golden',
        'format': 'string: How are you?\nint: 24324324\ndouble: 1234.560000\nbool: false',
        'to_delete': [{'birth': 2}],
        'some_sref': 'golden',
        'some_ref': {
            'double': 1234.56,
            'string': 'How are you?',
            'int': 24324324,
            'bool': False
        },
        'text': 'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.\n'
    }
}


_CONFIG_UPDATE_RESULT = {
    'another': 'world',
    'hello': {
        'bye': 'bye',
        'seq': [None],
        'dog': 'golden',
        'some_ref': {
            'double': 666.66,
            'bool': True,
            'int': -23,
            'string': 'Fine and, how are your cat?'
        },
        'format': 'string: Fine and, how are your cat?\nint: -23\ndouble: 666.660000\nbool: true',
        'to_delete': [{'birth': 2}],
        'some_sref': 'golden',
        'text': 'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.\n'
    }
}


class Test:
    def __init__(self, root_path: str):
        self._root_path = root_path
        self._client = None

    def set_client(self, client):
        self._client = client

    def labels(self) -> Dict[str, str]:
        raise NotImplementedError

    def prepare(self):
        raise NotImplementedError

    def do(self):
        raise NotImplementedError

    def _copy_config(self, name: str):
        shutil.rmtree(self._root_path)
        shutil.copytree(f"./resources/config/{name}", self._root_path)

    def _get_request(self, document: str, version: Optional[int] = None):
        return self._client.get(
            root_path=self._root_path,
            labels=self.labels(),
            document=document,
            version=version,
        )

    def _update_request(self, relative_paths: Optional[List[str]] = None):
        return self._client.update(self._root_path, relative_paths=relative_paths)

    def _watch_stream(self, uid: int, document: str):
        stream = self._client.watch()
        stream.watch(
            uid=uid,
            root_path=self._root_path,
            labels=self.labels(),
            document=document,
        )
        return stream

    def _trace_stream(self, document: str, labels: Optional[Dict[str, str]] = None):
        return self._client.trace(self._root_path, labels or self.labels(), document)


class GetTest(Test):
    def labels(self) -> Dict[str, str]:
        return {"low": "priority", "high": "priority"}

    def prepare(self):
        self._copy_config("base")

    def do(self):
        r = self._get_request('test')
        assert not r.logs
        assert r.version == 1
        assert r.checksum == b'K\x06\x00\xff\xa0\x1d\xbc\x11\xf4N\xa0\xb5\xed\xc8\xda\xda\x90\xe2\xd3l8-\xe5l\xe1|\\\x11_\x8e\xb6F'
        assert r.element == _CONFIG_BASE_RESULT

        return r.namespace_id


class UpdateTest(Test):
    def labels(self) -> Dict[str, str]:
        return {"low": "priority", "high": "priority"}

    def prepare(self):
        self._copy_config("base")

    def do(self):
        r1 = self._get_request("test")
        assert not r1.logs
        assert r1.version == 1
        assert r1.checksum == b'K\x06\x00\xff\xa0\x1d\xbc\x11\xf4N\xa0\xb5\xed\xc8\xda\xda\x90\xe2\xd3l8-\xe5l\xe1|\\\x11_\x8e\xb6F'
        assert r1.element == _CONFIG_BASE_RESULT

        r2 = self._update_request()
        assert r2.namespace_id == r1.namespace_id
        assert r2.version == 1

        self._copy_config("update")

        r3 = self._update_request()
        assert r3.namespace_id == r1.namespace_id
        assert r3.version == 2

        r4 = self._get_request("test")
        assert r4.namespace_id == r1.namespace_id
        assert not r4.logs
        assert r4.version == 2
        assert r4.checksum == b'w\xdf\x81}\xfba\xe5\x93\x938\x9f\xdei\xeb\x19_]w\xbe\xb6\xc2E\xf0\xbf\x1fY\xa4\xfb\xfan\x02u'
        assert r4.element == _CONFIG_UPDATE_RESULT


class WatchTest(Test):
    def labels(self) -> Dict[str, str]:
        return {"low": "priority", "high": "priority"}

    def prepare(self):
        self._copy_config("base")

    def do(self):
        stream = self._watch_stream(0, "test")
        it = iter(stream)

        r1 = next(it)
        assert r1.status == WatchStatus.OK
        assert r1.uid == 0
        assert not r1.value.logs
        assert r1.value.version == 1
        assert r1.value.checksum == b'K\x06\x00\xff\xa0\x1d\xbc\x11\xf4N\xa0\xb5\xed\xc8\xda\xda\x90\xe2\xd3l8-\xe5l\xe1|\\\x11_\x8e\xb6F'
        assert r1.value.element == _CONFIG_BASE_RESULT

        self._copy_config("update")

        r2 = self._update_request()
        assert r2.namespace_id == r1.value.namespace_id
        assert r2.version == 2

        r3 = next(it)
        assert r3.status == WatchStatus.OK
        assert r1.uid == 0
        assert not r1.value.logs
        assert r3.value.version == 2
        assert r3.value.checksum == b'w\xdf\x81}\xfba\xe5\x93\x938\x9f\xdei\xeb\x19_]w\xbe\xb6\xc2E\xf0\xbf\x1fY\xa4\xfb\xfan\x02u'
        assert r3.value.element == _CONFIG_UPDATE_RESULT

        stream.close()

        return r1.value.namespace_id


class TraceGetTest(GetTest):
    def do(self):
        stream = self._trace_stream("test")
        it = iter(stream)

        namespace_id = super().do()

        r = next(it)
        assert r.status == TraceStatus.RETURNED_ELEMENTS
        assert r.namespace_id == namespace_id
        assert r.version == 1
        assert r.labels == self.labels()
        assert r.document == "test"


class TraceWatchTest(WatchTest):
    def do(self):
        stream = self._trace_stream("test")
        it = iter(stream)

        namespace_id = super().do()

        sorted_responses = tuple(self._sort_responses(it))

        assert len(sorted_responses) == 4

        for r in sorted_responses:
            assert r.namespace_id == namespace_id
            assert r.labels == self.labels()
            assert r.document == "test"

        assert sorted_responses[0].status == TraceStatus.ADDED_WATCHER

        assert sorted_responses[1].status == TraceStatus.RETURNED_ELEMENTS
        assert sorted_responses[1].version == 1

        assert sorted_responses[2].status == TraceStatus.RETURNED_ELEMENTS
        assert sorted_responses[2].version == 2

        assert sorted_responses[3].status == TraceStatus.REMOVED_WATCHER

    def _sort_responses(self, traces_it):
        msgs_by_status = defaultdict(list)
        for _, m in zip(range(4), traces_it):
            msgs_by_status[m.status].append(m)
        for m in msgs_by_status.get(TraceStatus.ADDED_WATCHER, []):
            yield m
        it = sorted(
            msgs_by_status.get(TraceStatus.RETURNED_ELEMENTS, []),
            key=lambda m: m.version,
        )
        for m in it:
            yield m
        for m in msgs_by_status.get(TraceStatus.REMOVED_WATCHER, []):
            yield m


class StressTest(Test):
    def labels(self) -> Dict[str, str]:
        return {"low": "priority", "high": "priority"}

    def prepare(self):
        self._copy_config("base")

    def do(self):
        r = self._update_request()
        assert r.status == mhconfig_pb2.UpdateResponse.Status.OK
        assert r.version == 1

        for i in range(100000):
            r1 = self._get_request("other")
            assert r1.namespace_id == r.namespace_id
            assert not r1.logs
            assert r1.version == i*2 + 1
            assert r1.checksum == b'z\xe5\xa9#\xc6*kp(d\xbd\xee\\Z6X"\x90\xcdzXf$\xd7\xaf\x85\xc2SL\xa8}\xa8'

            self._copy_config("stress")

            r2 = self._update_request()
            assert r2.namespace_id == r.namespace_id
            assert r2.status == mhconfig_pb2.UpdateResponse.Status.OK
            assert r2.version == i*2 + 2

            r3 = self._get_request("other")
            assert r3.namespace_id == r.namespace_id
            assert not r3.logs
            assert r3.version == i*2 + 2
            assert r3.checksum == b'\xeb\x86~\x0f6\x04]T\xca\xc7cq\xc4\xbb[\xde\xa0\x08\xc2\xb7x\xab\xde\x1d\xc2\x9a\x8e\x05$W\x04\xbc'

            self._copy_config("base")

            r4 = self._update_request()
            assert r4.namespace_id == r.namespace_id
            assert r4.status == mhconfig_pb2.UpdateResponse.Status.OK
            assert r4.version == i*2 + 3


class Tester:
    def __init__(self, address: str, metadata: List[Tuple[str, str]]):
        self._address = address
        self._metadata = metadata
        self._tests = [
            GetTest,
            UpdateTest,
            WatchTest,
            TraceGetTest,
            TraceWatchTest,
            StressTest,
        ]

    def do(self):
        for test_cls in self._tests:
            self._do_test(test_cls)

    def _do_test(self, test_cls):
        with tempfile.TemporaryDirectory() as config_path:
            test = test_cls(config_path)

            with grpc.insecure_channel(self._address) as channel:
                client = Client(
                    mhconfig_pb2_grpc.MHConfigStub(channel),
                    metadata=self._metadata,
                )
                test.set_client(client)
                print("Running test '{}'".format(test_cls.__name__))
                test.prepare()
                test.do()


def main(address):
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=log_level)

    auth_token = os.environ.get('MHCONFIG_AUTH_TOKEN', '')

    metadata = (
        ('mhconfig-auth-token', auth_token),
    )

    tester = Tester(address, metadata=metadata)
    tester.do()


if __name__ == "__main__":
    main(*sys.argv[1:])
