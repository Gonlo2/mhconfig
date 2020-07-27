# Inotify updater
A simple inotify updater to notify the server about changes in the filesystem.

## Setup
First is necessary install the necessary modules and create the protobuf/grpc stub,
to do it with a virtual environment run:

```bash
virtualenv .env
. .env/bin/activate
./prepare.sh
```

## Usage
To execute it run for example

```bash
. .env/bin/activate
# MHCONFIG_AUTH_TOKEN=<auth token> ./updater.py <server address> <path to watch> <seconds of grace before trigger the update>
LOG_LEVEL=debug MHCONFIG_AUTH_TOKEN=test ./updater.py 127.0.0.1:2222 ../../examples/config/ 0.2
```
