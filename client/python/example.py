import sys
import os
import time
import grpc
from mhconfig.client import Client, NamespaceClient, ConfigKey, NamespaceKey, make_session
from mhconfig.proto import mhconfig_pb2_grpc
from pprint import pprint


def main(address, n, document, root_path, overrides, flavors):
    auth_token = os.environ.get('MHCONFIG_AUTH_TOKEN', '')
    overrides = overrides.split(':') if overrides else []
    flavors = flavors.split(':') if flavors else []

    channel = grpc.insecure_channel(address)
    stub = mhconfig_pb2_grpc.MHConfigStub(channel)

    client = Client(stub, metadata=(('mhconfig-auth-token', auth_token),))
    client.init()

    namespace_key = NamespaceKey(
        root_path=root_path,
        overrides=tuple(overrides),
    )

    namespace_client = NamespaceClient(client, namespace_key)

    k = ConfigKey(document=document, flavors=tuple(flavors))

    #test_get(namespace_client, k, int(n))
    test_watch(namespace_client, k, int(n))
    #test_session(namespace_client, k, int(n))
    #try:
        #test_get(namespace_client, k, int(n))
        #test_watch(namespace_client, k, int(n))
        #test_session(namespace_client, k, int(n))
    #finally:
        #client.close()
        #channel.close()


def test_get(client, config_key, n):
    v = None
    for _ in range(n):
        r = client.get(config_key, v)
        #_, v, _ = r
        #session = client.new_session()
        #config = session.get(document, flavors=flavors)
        pprint(r)
        time.sleep(1)


def test_watch(client, config_key, n):
    def on_update(status, version_key, specific_config):
        print("#" * 120)
        print(status, version_key)
        pprint(specific_config)

    watcher_uid, callback_uid = client.watch(config_key, on_update)
    print(watcher_uid, callback_uid)
    time.sleep(4)
    print("To unwatch")
    removed = client.unwatch(watcher_uid, callback_uid)
    print("Unwatched!!", removed)
    time.sleep(10000)


def test_session(client, config_key, n):
    for i in range(n):
        print("Loop", i)
        session = make_session(client)
        status, specific_config = session.get(config_key)
        print(status)
        pprint(specific_config)
        time.sleep(0.01)


if __name__ == '__main__':
    main(*sys.argv[1:])
