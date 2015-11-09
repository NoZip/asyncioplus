import sys

from io import DEFAULT_BUFFER_SIZE
from asyncio import coroutine

PYTHON_3_5 = sys.version_info >= (3, 5)

if not PYTHON_3_5:
    class StopAsyncIteration(Exception):
        pass

class BlockReaderIterator:
    def __init__(self, reader, size = None, block_size = DEFAULT_BUFFER_SIZE):
        self._reader = reader
        self._size = size
        self._block_size = block_size

    @coroutine
    def __aiter__(self):
        if self._size:
            self._block_count, self._last_block_size = divmod(self._size, self._block_size)

        self._block_index = 0

        return self

    @coroutine
    def __anext__(self):
        if self._size and self._block_index > self._block_count:
            raise StopAsyncIteration

        block_size = self._block_size

        if self._size and self._block_index == self._block_count:
            if self._last_block_size == 0:
                raise StopAsyncIteration

            block_size = self._last_block_size

        block = yield from self._reader.read(block_size)

        if block == b"":
            raise StopAsyncIteration

        self._block_index += 1

        return block


class DelimiterReaderIterator:
    def __init__(self, reader, delimiter = b"\n"):
        self._reader = reader
        self._delimiter = delimiter

    @coroutine
    def __aiter__(self):
        return self

    @coroutine
    def __anext__(self):
        chunk = yield from self;_reader.read_until(self._delimiter)

        if chunk == b"":
            raise StopAsyncIteration

        return chunk
