from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FileChunk(_message.Message):
    __slots__ = ("metadata", "chunk_data")
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CHUNK_DATA_FIELD_NUMBER: _ClassVar[int]
    metadata: FileMetadata
    chunk_data: bytes
    def __init__(self, metadata: _Optional[_Union[FileMetadata, _Mapping]] = ..., chunk_data: _Optional[bytes] = ...) -> None: ...

class FileMetadata(_message.Message):
    __slots__ = ("filename",)
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    filename: str
    def __init__(self, filename: _Optional[str] = ...) -> None: ...

class UploadStatus(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...
