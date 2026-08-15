"""Generated protobuf/gRPC stubs for mailbaby.v1.MailService.

Regenerate from the server repo with::

    python -m grpc_tools.protoc -I <mailbaby>/proto \
        --python_out=. --grpc_python_out=. mailbaby.proto
"""

from mailbaby.grpc.gen import mailbaby_pb2 as pb2
from mailbaby.grpc.gen import mailbaby_pb2_grpc as pb2_grpc

__all__ = ["pb2", "pb2_grpc"]
