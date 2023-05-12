import asyncio
import functools
import time

import aiofile
import uvloop


def print_on_call_decorator(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        print("real read called")
        value = func(*args, **kwargs)
        return value

    return wrapper_decorator


aiofile.AIOFile.read_bytes = print_on_call_decorator(aiofile.AIOFile.read_bytes)


async def main():
    async with aiofile.AIOFile("test_line_iter_file", "r") as f:
        last_line_time = time.perf_counter()
        async for line in aiofile.LineReader(f, chunk_size=aiofile.LineReader.CHUNK_SIZE * 16*16):
            print("line_time", time.perf_counter() - last_line_time)
            last_line_time = time.perf_counter()
            print(line, end="")


if __name__ == "__main__":
    open("test_line_iter_file", "w").write("\n".join(str(i) for i in range(1000000)))
    asyncio.run(main())
