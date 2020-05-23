#!/usr/bin/env bash
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mhconfig/proto/mhconfig.proto
