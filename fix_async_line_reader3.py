import asyncio
import asyncio
import collections.abc
import functools
import itertools
import time
from typing import Union

import aiofile


async def main():
    async with aiofile.async_open("test_line_iter_file", "r") as f:
        last_line_time = time.perf_counter()
        async for line in f:
            print("line_time", time.perf_counter() - last_line_time)
            last_line_time = time.perf_counter()
            print(line, end="")


if __name__ == "__main__":
    open("test_line_iter_file", "w").write("\n".join(str(i) for i in range(10000)))
    start = time.perf_counter()
    asyncio.run(main())
    print("end_time", time.perf_counter() - start)
