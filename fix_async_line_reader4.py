import asyncio
import asyncio
import collections.abc
import functools
import io
import itertools
import time
from typing import Union, Self

import aiofile


class CustomLineReader(collections.abc.AsyncIterable):
    CHUNK_SIZE = 4192

    def __init__(
        self,
        aio_file: aiofile.AIOFile,
        offset: int = 0,
        chunk_size: int = CHUNK_SIZE,
        line_sep: str = "\n",
    ):
        self.__reader = aiofile.Reader(aio_file, chunk_size=chunk_size, offset=offset)

        self._buffer = None

        self.linesep = aio_file.encode_bytes(line_sep) if aio_file.mode.binary else line_sep

        self.chunk_iterator = None
        self.last_read = None

    async def setup_buffer(self, buffer_initialization=None):
        chunk = await self.__reader.read_chunk()
        if not chunk:
            raise StopAsyncIteration(chunk)

        if self._buffer:
            self._buffer.close()
            del self._buffer
        self._buffer = io.BytesIO() if self.__reader.file.mode.binary else io.StringIO()
        if buffer_initialization:
            self._buffer.write(buffer_initialization)

        self._buffer.write(chunk)
        self._buffer.seek(0)

        self.chunk_iterator = self._buffer.__iter__()

    async def __anext__(self) -> Union[bytes, str]:
        if not self._buffer:
            await self.setup_buffer()
        try:
            self.last_read = next(self.chunk_iterator)
            if self.last_read[-1] != "\n":
                await self.setup_buffer(self.last_read)
                self.last_read = next(self.chunk_iterator)
        except StopIteration:
            await self.setup_buffer(self.last_read)
            self.last_read = next(self.chunk_iterator)
        return self.last_read


    def __aiter__(self) -> Self:
        return self


# def print_on_call_decorator(func):
#     @functools.wraps(func)
#     def wrapper_decorator(*args, **kwargs):
#         print("real read called")
#         value = func(*args, **kwargs)
#         return value
#
#     return wrapper_decorator
#
#
# aiofile.AIOFile.read_bytes = print_on_call_decorator(aiofile.AIOFile.read_bytes)


async def main():
    async with aiofile.AIOFile("test_line_iter_file", "r") as f:
        last_line_time = time.perf_counter()
        async for line in CustomLineReader(f, chunk_size=aiofile.LineReader.CHUNK_SIZE):
            # print("line_time", time.perf_counter() - last_line_time)
            last_line_time = time.perf_counter()
            # print(line, end="")


if __name__ == "__main__":
    open("test_line_iter_file", "w").write("\n".join(str(i) for i in range(100000)))
    start = time.perf_counter()
    asyncio.run(main())
    print("end_time", time.perf_counter() - start)
