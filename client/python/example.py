import sys
import time
from mhconfig.client import Client
from pprint import pprint


def main(address, n, document, root_path, *overrides):
    client = Client(address, root_path, overrides)
    client.init()

    try:
        for _ in range(int(n)):
            session = client.new_session()
            config = session.get(document)
            pprint(config)
            time.sleep(1)
    finally:
        client.close()


if __name__ == '__main__':
    main(*sys.argv[1:])
