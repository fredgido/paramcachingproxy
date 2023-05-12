import asyncio
import asyncio
import collections.abc
import functools
import itertools
import time
from typing import Union

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

        self._buffers = []

        self.linesep = aio_file.encode_bytes(line_sep) if aio_file.mode.binary else line_sep

        self.binary_mode = aio_file.mode.binary

        self.last_buffer_position = 0
        self.last_buffer_position_is_in_previous_buffer = 0

    async def readline(self) -> Union[str, bytes]:
        index = None
        while not index:
            if not self._buffers:
                self._buffers.append(await self.__reader.read_chunk())
                self.last_buffer_position = 0
                continue
            try:
                index = self._buffers[-1][self.last_buffer_position :].index(self.linesep)
            except ValueError:
                index = False
            if not index:
                new_read = await self.__reader.read_chunk()
                if len(new_read) == 0:
                    if self._buffers:
                        if self._buffers[-1]:
                            index = len(self._buffers[-1])
                        else:
                            self._buffers = []
                        break
                self._buffers.append(await self.__reader.read_chunk())
                self.last_buffer_position = 0
                continue

        if (buffer_count := len(self._buffers)) == 1:
            line = self._buffers[-1][self.last_buffer_position : self.last_buffer_position + index + 1]
        elif buffer_count == 0:
            return "" if not self.binary_mode else b""
        else:
            join_base = "" if not self.binary_mode else b""
            line = join_base.join(
                itertools.chain(
                    self._buffers[0][self.last_buffer_position :],
                    self._buffers[1:-1],
                    self._buffers[-1][: self.last_buffer_position + index + 1],
                )
            )
            self._buffers = [self._buffers[-1]]
        self.last_buffer_position += index + 1
        return line

    async def __anext__(self) -> Union[bytes, str]:
        line = await self.readline()

        if not line:
            raise StopAsyncIteration(line)

        return line

    def __aiter__(self) -> "LineReader":
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
        async for line in CustomLineReader(f, chunk_size=aiofile.LineReader.CHUNK_SIZE *16*16 ):
            #print("line_time", time.perf_counter() - last_line_time)
            last_line_time = time.perf_counter()
            #print(line, end="")


if __name__ == "__main__":
    open("test_line_iter_file", "w").write("\n".join(str(i) for i in range(100000)))
    start = time.perf_counter()
    asyncio.run(main())
    print("end_time", time.perf_counter() - start)
