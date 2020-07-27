# Python client
A simple client to use the mhconfig server from a python program.

## Setup
First is necessary install the necessary modules and create the protobuf/grpc stub,
to do it with a virtual environment run:

```bash
virtualenv .env
. .env/bin/activate
./prepare.sh
```

## Example usage
To execute the example run run

```bash
. .env/bin/activate
MHCONFIG_AUTH_TOKEN=test python example.py 127.0.0.1:2222 10000000000 my_document /mnt/data/mhconfig low-priority:high-priority ''
```
