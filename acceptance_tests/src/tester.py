#!/usr/bin/env python3
import logging
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from queue import Queue
from typing import Any, List, Optional, Tuple

import grpc
from mhconfig.client import decode
from mhconfig.proto import mhconfig_pb2, mhconfig_pb2_grpc

logger = logging.getLogger(__name__)


class IterableQueue:
    def __init__(self):
        self._q = Queue()

    def put(self, o: Any):
        self._q.put((False, o))

    def close(self):
        self._q.put((True, None))

    def it(self):
        while True:
            exit, o = self._q.get()
            if exit:
                break
            yield o


@dataclass
class GetResponse:
    status: mhconfig_pb2.GetResponse.Status
    namespace_id: int
    version: int
    value: Any
    checksum: bytes


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
        'seq': (None,),
        'dog': 'golden',
        'format': 'string: How are you?\nint: 24324324\ndouble: 1234.560000\nbool: false',
        'to_delete': ({'birth': 2},),
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
        'seq': (None,),
        'dog': 'golden',
        'some_ref': {
            'double': 666.66,
            'bool': True,
            'int': -23,
            'string': 'Fine and, how are your cat?'
        },
        'format': 'string: Fine and, how are your cat?\nint: -23\ndouble: 666.660000\nbool: true',
        'to_delete': ({'birth': 2},),
        'some_sref': 'golden',
        'text': 'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.\n'
    }
}


class Test:
    def __init__(
            self,
            root_path: str,
            metadata: List[Tuple[str, str]]
    ):
        self._root_path = root_path
        self._stub = None
        self._metadata = metadata

    def set_stub(
            self,
            stub: mhconfig_pb2_grpc.MHConfigStub,
    ):
        self._stub = stub

    def overrides(self) -> List[str]:
        raise NotImplementedError

    def prepare(self):
        raise NotImplementedError

    def do(self):
        raise NotImplementedError

    def _copy_config(self, name: str):
        shutil.rmtree(self._root_path)
        shutil.copytree(f"./resources/config/{name}", self._root_path)

    def _get_request(
            self,
            document: str,
            version: Optional[int] = None,
            flavors: Optional[List[str]] = None
    ):
        request = mhconfig_pb2.GetRequest(
            root_path=self._root_path,
            overrides=self.overrides(),
            document=document,
            version=version or 0,
            flavors=flavors or [],
        )
        response = self._stub.Get(request, metadata=self._metadata)

        return GetResponse(
            status=response.status,
            namespace_id=response.namespace_id,
            version=response.version,
            value=decode(response.elements),
            checksum=response.checksum,
        )

    def _update_request(self, relative_paths: Optional[List[str]] = None):
        request = mhconfig_pb2.UpdateRequest(
            root_path=self._root_path,
            relative_paths=relative_paths or [],
            reload=relative_paths is None,
        )
        return self._stub.Update(request, metadata=self._metadata)

    def _watch_stream(self, q):
        for r in self._stub.Watch(q.it(), metadata=self._metadata):
            yield WatchResponse(
                status=r.status,
                namespace_id=r.namespace_id,
                version=r.version,
                value=decode(r.elements),
                checksum=r.checksum,
            )

    def _make_watch_request(
            self,
            uid: int,
            document: str,
            flavors: Optional[List[str]] = None
    ):
        return mhconfig_pb2.WatchRequest(
            uid=uid,
            root_path=self._root_path,
            overrides=self.overrides(),
            document=document,
            flavors=flavors or [],
        )

    def _trace_stream(
            self,
            document: str,
            overrides: Optional[List[str]] = None,
            flavors: Optional[List[str]] = None
    ):
        request = mhconfig_pb2.TraceRequest(
            root_path=self._root_path,
            overrides=overrides or self.overrides(),
            document=document,
            flavors=flavors or [],
        )
        return self._stub.Trace(request, metadata=self._metadata)

class GetTest(Test):
    def overrides(self) -> List[str]:
        return ["low-priority", "high-priority"]

    def prepare(self):
        self._copy_config("base")

    def do(self):
        r = self._get_request('test')
        assert r.status == mhconfig_pb2.GetResponse.Status.OK
        assert r.version == 1
        assert r.checksum == b'\x1c\xbc\xdf\xb2\x04\xea)\xe6-\xbc]\x1c\x89+C\xa7\x0f\xecI\x1c>\xd9\xe3\xba\xea3E}?\x97\xf5\x7f'
        assert r.value == _CONFIG_BASE_RESULT

        return r.namespace_id


class UpdateTest(Test):
    def overrides(self) -> List[str]:
        return ["low-priority", "high-priority"]

    def prepare(self):
        self._copy_config("base")

    def do(self):
        r1 = self._get_request("test")
        assert r1.status == mhconfig_pb2.GetResponse.Status.OK
        assert r1.version == 1
        assert r1.checksum == b'\x1c\xbc\xdf\xb2\x04\xea)\xe6-\xbc]\x1c\x89+C\xa7\x0f\xecI\x1c>\xd9\xe3\xba\xea3E}?\x97\xf5\x7f'
        assert r1.value == _CONFIG_BASE_RESULT

        r2 = self._update_request()
        assert r2.namespace_id == r1.namespace_id
        assert r2.version == 1

        self._copy_config("update")

        r3 = self._update_request()
        assert r3.namespace_id == r1.namespace_id
        assert r3.version == 2

        r4 = self._get_request("test")
        assert r4.namespace_id == r1.namespace_id
        assert r4.status == mhconfig_pb2.UpdateResponse.Status.OK
        assert r4.version == 2
        assert r4.checksum == b'\x1c\xfa\xa1\xd8\xca>^\xceG!\x01\x9e\x97P\x88\xec\xa2i>\x8c\xe4\xd4SQ\x8c\x96\x02\x1bIh#\xed'
        assert r4.value == _CONFIG_UPDATE_RESULT

class WatchTest(Test):
    def overrides(self) -> List[str]:
        return ["low-priority", "high-priority"]

    def prepare(self):
        self._copy_config("base")

    def do(self):
        q = IterableQueue()
        it = self._watch_stream(q)

        q.put(self._make_watch_request(0, "test"))

        r1 = next(it)
        assert r1.status == mhconfig_pb2.GetResponse.Status.OK
        assert r1.version == 1
        assert r1.checksum == b'\x1c\xbc\xdf\xb2\x04\xea)\xe6-\xbc]\x1c\x89+C\xa7\x0f\xecI\x1c>\xd9\xe3\xba\xea3E}?\x97\xf5\x7f'
        assert r1.value == _CONFIG_BASE_RESULT

        self._copy_config("update")

        r2 = self._update_request()
        assert r2.namespace_id == r1.namespace_id
        assert r2.version == 2

        r3 = next(it)
        assert r3.status == mhconfig_pb2.UpdateResponse.Status.OK
        assert r3.version == 2
        assert r3.checksum == b'\x1c\xfa\xa1\xd8\xca>^\xceG!\x01\x9e\x97P\x88\xec\xa2i>\x8c\xe4\xd4SQ\x8c\x96\x02\x1bIh#\xed'
        assert r3.value == _CONFIG_UPDATE_RESULT

        q.close()

        return r1.namespace_id


class TraceGetTest(GetTest):
    def do(self):
        it = self._trace_stream("test")

        namespace_id = super().do()

        r = next(it)
        assert r.status == mhconfig_pb2.TraceResponse.Status.RETURNED_ELEMENTS
        assert r.namespace_id == namespace_id
        assert r.version == 1
        assert r.overrides == self.overrides()
        assert len(r.flavors) == 0
        assert r.document == "test"


class TraceWatchTest(WatchTest):
    def do(self):
        it = self._trace_stream("test")

        namespace_id = super().do()

        sorted_responses = tuple(self._sort_responses(it))

        assert len(sorted_responses) == 4

        for r in sorted_responses:
            assert r.namespace_id == namespace_id
            assert r.overrides == self.overrides()
            assert len(r.flavors) == 0
            assert r.document == "test"
            assert r.peer == sorted_responses[0].peer

        assert sorted_responses[0].status == mhconfig_pb2.TraceResponse.Status.ADDED_WATCHER

        assert sorted_responses[1].status == mhconfig_pb2.TraceResponse.Status.RETURNED_ELEMENTS
        assert sorted_responses[1].version == 1

        assert sorted_responses[2].status == mhconfig_pb2.TraceResponse.Status.RETURNED_ELEMENTS
        assert sorted_responses[2].version == 2

        assert sorted_responses[3].status == mhconfig_pb2.TraceResponse.Status.REMOVED_WATCHER

    def _sort_responses(self, traces_it):
        msgs_by_status = defaultdict(list)
        for _, m in zip(range(4), traces_it):
            msgs_by_status[m.status].append(m)
        for m in msgs_by_status.get(mhconfig_pb2.TraceResponse.Status.ADDED_WATCHER, []):
            yield m
        it = sorted(
            msgs_by_status.get(mhconfig_pb2.TraceResponse.Status.RETURNED_ELEMENTS, []),
            key=lambda m: m.version,
        )
        for m in it:
            yield m
        for m in msgs_by_status.get(mhconfig_pb2.TraceResponse.Status.REMOVED_WATCHER, []):
            yield m


class StressTest(Test):
    def overrides(self) -> List[str]:
        return ["low-priority", "high-priority"]

    def prepare(self):
        self._copy_config("base")

    def do(self):
        r = self._update_request()
        assert r.status == mhconfig_pb2.UpdateResponse.Status.OK
        assert r.version == 1

        for i in range(1000):
            r1 = self._get_request("other")
            assert r1.namespace_id == r.namespace_id
            assert r1.status == mhconfig_pb2.GetResponse.Status.OK
            assert r1.version == i*2 + 1
            assert r1.checksum == b'\x9a\xad\xf1\xf2\xbd\x19\xa9CFj\xd4n&\xde&\xa9\x8a\xd0mm\x12\xb5\xb1#\x058\xbd\x00\xfd\xd3\x8d\xea'

            self._copy_config("stress")

            r2 = self._update_request()
            assert r2.namespace_id == r.namespace_id
            assert r2.status == mhconfig_pb2.UpdateResponse.Status.OK
            assert r2.version == i*2 + 2

            r3 = self._get_request("other")
            assert r3.namespace_id == r.namespace_id
            assert r3.status == mhconfig_pb2.GetResponse.Status.OK
            assert r3.version == i*2 + 2
            assert r3.checksum == b'U\xaaT:\xc1\xccW\xc5GV>\xf3R\x97\x8f\xd1\xa9\to5\xe5\xa2\x83(\x06\xcdc\xe7\xea{\xee\xf8'

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
            test = test_cls(config_path, self._metadata)

            with grpc.insecure_channel(self._address) as channel:
                test.set_stub(mhconfig_pb2_grpc.MHConfigStub(channel))
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
