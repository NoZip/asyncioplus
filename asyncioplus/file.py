import asyncio
import io
import os

from asyncio import coroutine

class FileBase:
    def __init__(self, file_stream, loop=None):
        file_stats = os.stat(file_stream.name)

        self._loop = loop or asyncio.get_event_loop()
        self._file_stream = file_stream
        self._file_size = file_stats.st_size
        self._block_size = file_stats.st_blksize or io.DEFAULT_BUFFER_SIZE


class FileReader(FileBase):
    @coroutine
    def read(self, count):
        assert not self._file_stream.closed
        assert count >= 0

        if count == 0:
            return b""

        data = bytearray()
        block_count, last_block_size = divmod(count, self._block_size)
        for block_index in range(block_count):
            block = self._file_stream.read(self._block_size)
            data.extend(block)
            yield from asyncio.sleep(0)

        if last_block_size:
            block = self._file_stream.read(self._block_size)
            data.extend(block)

        return bytes(data)

    @coroutine
    def read_until(self, delimiter):
        assert not self._file_stream.closed
        assert isinstance(delimiter, bytes)

        data = bytearray()
        while True:
            block = self._file_stream.read(self._block_size)

            if not block:
                return bytes(data)

            data.extend(block)

            search_length = search_length = len(block) - len(delimiter) - 1
            index = data.find(delimiter, start=-search_length)

            if index >= 0:
                offset = len(data) - index - len(delimiter)
                self._file_stream.seek(-offset, io.SEEK_CUR)

                return bytes(data[:index])

            yield from asyncio.sleep(0)


class FileWriter(FileBase):
    def __init__(self, file_stream, loop=None):
        super().__init__(file_stream, loop)

        self._listening = True
        self._listen_task = self._loop.create_task(self._listen())
        self._pending = []

    def __del__(self):
        self.close()

    @coroutine
    def _listen(self):
        while self._listening:
            while self._pending:
                data = self._pending.pop(0)
                self._file_stream.write(data)
                yield from asyncio.sleep(0)

            yield from asyncio.sleep(0.5)

    @coroutine
    def _close(self):
        while self._pending:
            data = self._pending.pop(0)
            self._file_stream.write(data)

            if self._loop.is_running():
                yield from asyncio.sleep(0)

        self._file_stream.close()

    def close(self):
        if self._listening:
            self._listening = False
            self._listen_task.cancel()

            if self._loop.is_running():
                self._loop.create_task(self._close())
            else:
                self._listen_task.cancel()

                while self._pending:
                    data = self._pending.pop(0)
                    self._file_stream.write(data)

                self._file_stream.close()

    def write(self, data):
        assert self._listening
        assert not self._file_stream.closed
        assert isinstance(data, bytes)

        if data == b"":
            return

        data_stream = io.BytesIO(data)
        block_count, last_block_size = divmod(len(data), self._block_size)

        for block_index in range(block_count):
            block = data_stream.read(self._block_size)
            self._pending.append(block)

        if last_block_size:
            block = data_stream.read(last_block_size)
            self._pending.append(block)

    @coroutine
    def drain(self):
        assert self._listening
        assert not self._file_stream.closed

        while self._pending:
            yield from asyncio.sleep(0.1)


class File(FileReader, FileWriter):
    @staticmethod
    def open(filename, mode="w+b"):
        loop = asyncio.get_event_loop()

        if "b" not in mode:
            mode += "b"

        file = open(filename, mode)

        if "+" in mode:
            return File(file, loop=loop)
        elif "r" in mode:
            return FileReader(file, loop=loop)
        elif "w" in mode:
            return FileWriter(file, loop=loop)
