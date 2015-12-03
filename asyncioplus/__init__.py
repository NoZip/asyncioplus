from .stream import open_connection, start_server
from .stream import StreamReader, StreamWriter, StreamingProtocol
from .file import File


__version__ = "0.2"


if "StopAsyncIteration" not in dir(__builtins__):
    class StopAsyncIteration(Exception):
        pass
