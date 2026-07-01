#!/usr/bin/env bash
if [ -n "$SDE_INSTALL" ]; then
py=$SDE_INSTALL/bin/python3.8
else 
py=python3
fi
$py -m grpc_tools.protoc -I . --grpc_python_out . --pyi_out . --python_out . netccl.proto