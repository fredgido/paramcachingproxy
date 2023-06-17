import asyncio
import collections
import datetime
import io
import json
import os
import pathlib
import random
import re
import time
import typing
import urllib.parse
import uuid
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, Union

import aiofile
import aiofiles
import httpx
import renameat2
import uvicorn
from asgiref.typing import HTTPScope
import h2.events

import handle_keyboard_interrupt
from aio_safe_rename import aio_safe_rename
from disk_cache import async_add_to_cache, async_exists_in_cache

CoroutineFunction = Callable[[Union[dict, type(None)]], Awaitable]

path_settings = os.getenv("MEDIA_FOLDER", ".")
twitter_media_path = pathlib.Path(f"{path_settings}/twitter_media")
twitter_media_path.mkdir(exist_ok=True)
print(twitter_media_path)

twitter_url_regex_str = r"(?P<subdomain>[^.\/]*)\.twimg\.com\/(?P<type>tweet_video_thumb|media|tweet_video)/(?P<name>[a-zA-Z\d_-]*)(?:\?format=|\.)(?P<extension>[a-zA-Z0-9]{3,4})"
twitter_url_regex = re.compile(twitter_url_regex_str)

path_settings = os.getenv("LOGS_FOLDER", ".")
logs_path = pathlib.Path(f"{path_settings}/logs")
logs_path.mkdir(exist_ok=True)
logs_file = open(logs_path / f"files_{datetime.datetime.utcnow().isoformat()}.txt", mode="w")  # todo async file pool


httpx_client_storage = dict[asyncio.AbstractEventLoop, httpx.AsyncClient]()
httpx_client_storage_lock = asyncio.Lock()


# async def get_httpx_client() -> httpx.AsyncClient:
#     current_loop = asyncio.get_running_loop()
#     httpx_client = httpx_client_storage.get(current_loop)
#     if not httpx_client:
#         async with httpx_client_storage_lock:
#             httpx_client = httpx_client_storage.get(current_loop)
#             if not httpx_client:
#                 httpx_client = httpx.AsyncClient(
#                     timeout=61,
#                     http2=True,
#                     limits=httpx.Limits(
#                         max_connections=200,
#                         max_keepalive_connections=100,
#                     ),
#                 )
#                 httpx_client_storage[current_loop] = httpx_client
#     return httpx_client


async def get_httpx_client() -> httpx.AsyncClient:
    current_loop = asyncio.get_running_loop()
    httpx_client = httpx_client_storage.get(current_loop)
    if not httpx_client:
        async with httpx_client_storage_lock:
            httpx_client = httpx_client_storage.get(current_loop)
            if not httpx_client:
                httpx_client = (
                    httpx.AsyncClient(
                        timeout=61,
                        http2=True,
                        limits=httpx.Limits(
                            max_connections=200,
                            max_keepalive_connections=100,
                        ),
                    ),
                    httpx.AsyncClient(
                        timeout=61,
                        http2=True,
                        limits=httpx.Limits(
                            max_connections=200,
                            max_keepalive_connections=100,
                        ),
                    ),
                    httpx.AsyncClient(
                        timeout=61,
                        http2=True,
                        limits=httpx.Limits(
                            max_connections=200,
                            max_keepalive_connections=100,
                        ),
                    ),
                )
                httpx_client_storage[current_loop] = httpx_client
    return httpx_client[random.randint(0, 2)]


def twitter_url_to_orig(twitter_url: str):
    match = twitter_url_regex.search(twitter_url)
    if not match:
        return None, None, None, None

    capture_groups = match.groupdict()
    return capture_groups["subdomain"], capture_groups["type"], capture_groups["name"], capture_groups["extension"]


twitter_video_url_regex_str = r"(?P<subdomain>[^.\/]*)\.twimg\.com\/(?P<type>ext_tw_video|amplify_video)/(?P<id>[\d]*)/(?P<pu>pu/|pr/)?vid/(?P<resolution>[x\d]*)/(?P<name>[a-zA-Z\d_-]*)(?:\?format=|\.)(?P<extension>[a-zA-Z0-9]{3,4})"
twitter_video_url_regex = re.compile(twitter_video_url_regex_str)


def twitter_video_url_to_orig(twitter_url: str):
    match = twitter_video_url_regex.search(twitter_url)
    if not match:
        return None, None, None, None, None, None, None

    capture_groups = match.groupdict()
    return (
        capture_groups["subdomain"],
        capture_groups["type"],
        capture_groups["id"],
        capture_groups["pu"],
        capture_groups["name"],
        capture_groups["extension"],
        capture_groups["resolution"],
    )


ext_content_type = {
    "gif": b"image/gif",
    "jpeg": b"image/jpeg",
    "jpg": b"image/jpeg",
    "png": b"image/png",
    "mp4": b"video/mp4",
}

CHUNK_SIZE = int(65536)


async def download_file_write_file(download_url: str, file_path: str | pathlib.Path):
    start_download = time.perf_counter()
    file_path_placeholder = str(file_path) + str(uuid.uuid4())
    async with (await get_httpx_client()).stream("GET", download_url) as response:  # todo save headers
        if response.status_code != 200:
            if response.status_code in (404,403):
                print("fail", response.status_code,download_url)
                await async_add_to_cache("missing_files_remote",download_url)
                return
            print(f"unkown status code {response.status_code}")
            return
        # async with aiofiles.open(file_path_placeholder, "wb") as aio_file:
        # print(file_path.name, " open file ", time.perf_counter() - start_download)
        async with aiofile.async_open(file_path_placeholder, "wb") as aio_file:
            # print(file_path.name, " open conection ", time.perf_counter() - start_download)



            response_iterator = response.aiter_bytes(chunk_size=CHUNK_SIZE * 4 * 8)

            async def wrap_read_response_stop_iter(future) -> Optional[bytes]:
                try:
                    return await future
                except StopAsyncIteration:
                    return None

            response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

            read_chunk = await response_future
            # print(file_path.name, " first chunk ", time.perf_counter() - start_download)
            while True:
                # print(file_path.name, " chunk loop ", time.perf_counter() - start_download)

                if not read_chunk:
                    break

                file_write_future = aio_file.write(read_chunk)
                response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

                read_chunk, file_write_result = await asyncio.gather(
                    response_future,
                    file_write_future,
                )
                if not read_chunk:
                    break

    # print(file_path.name, " download took ", time.perf_counter() - start_download)
    try:
        # renameat2.rename(file_path_placeholder, file_path, replace=False)
        await aio_safe_rename(file_path_placeholder, file_path, replace=False)
    except FileExistsError as e:
        print("failed renameat2", e)
    # await aiofiles.os.rename(file_path_placeholder, file_path)
    print(file_path.name, " donwload and rename took ", time.perf_counter() - start_download)


async def check_file_exists_async(file_exists_path):
    try:
        await (await aiofile.AIOFile(file_exists_path, "r")).close()
    except FileNotFoundError as e:
        return False
    return True


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

    def __aiter__(self) -> typing.Self:
        return self


class TaskGroupWithSemaphore(asyncio.TaskGroup):
    def __init__(self, max_concurrent_tasks: int = 60):
        super().__init__()
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def sem_task(self, coro):
        start = time.perf_counter()
        async with self.semaphore:
            # print("task ", coro, " waited ", time.perf_counter() - start, " for lock")
            return await coro

    def create_task(self, coro, *, name=None, context=None):
        return super().create_task(self.sem_task(coro), name=None, context=None)

    async def create_task_semaphored(self, coro, *, name=None, context=None):
        while self.semaphore.locked():
            await asyncio.sleep(0.5)

        return super().create_task(self.sem_task(coro), name=None, context=None)


async def main():
    async with aiofile.AIOFile(f"temp/downloaded_{datetime.datetime.utcnow().isoformat()}.txt", "a") as downloaded:
        async with aiofile.AIOFile("temp/asset_urls5.csv", "r") as f:
            async with TaskGroupWithSemaphore(5) as tg:
                last_line_time = time.perf_counter()
                async for line in CustomLineReader(f, chunk_size=aiofile.LineReader.CHUNK_SIZE * 16):
                    # print("keyboard ",handle_keyboard_interrupt.mutable["is_requested"])
                    if handle_keyboard_interrupt.mutable["is_requested"]:
                        break

                    # print("line_time", time.perf_counter() - last_line_time)
                    last_line_time = time.perf_counter()
                    line: str = line.strip()

                    subdomain, url_type, name, extension = twitter_url_to_orig(line)
                    if not name:
                        (
                            subdomain,
                            url_type,
                            video_id,
                            pu,
                            name,
                            extension,
                            resolution,
                        ) = twitter_video_url_to_orig(line)
                        if not name:
                            print(line, "failed")
                            continue
                        # print("video")
                        # continue
                    # if type == "video":
                    #     print("skip video")

                    # downloaded
                    # if "tweet_video_thumb" in line:
                    #     print("skip thumb")

                    file_name = f"{name}.{extension}"
                    file_path = twitter_media_path / file_name

                    start = time.perf_counter()
                    # if await check_file_exists_async(file_path):
                    if file_path.exists():
                        # print("exists", line)
                        # print("exist_check", time.perf_counter() - start)
                        continue
                    # print("exist_check", time.perf_counter() - start)
                    print("doesn't exist", line)

                    if url_type == "ext_tw_video":
                        download_url = f"https://{subdomain}.twimg.com/{url_type}/{video_id}/{pu}vid/{resolution}/{name}.{extension}"
                    else:
                        if subdomain == "video":
                            download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}.{extension}"
                        else:
                            download_url = (
                                f"https://{subdomain}.twimg.com/{url_type}/{name}?format={extension}&name=orig"
                            )

                    async def download(download_file_path, download_download_url, line_nr):
                        start_time = time.perf_counter()
                        try:
                            await download_file_write_file(download_download_url, download_file_path)
                        # except (httpx.RemoteProtocolError, httpx.RequestError,):
                        #     async with httpx_client_storage_lock:
                        #         httpx_client_storage.clear()
                        #     await download_file_write_file(download_download_url, download_file_path)
                        # except Exception as e:
                        #     print("error_class", e.__class__,"|||", e)
                        #     async with httpx_client_storage_lock:
                        #         httpx_client_storage.clear()
                        #     raise Exception
                        #     #await download_file_write_file(download_download_url, download_file_path)
                        except BaseException as e:  # asyncio.exceptions.CancelledError
                            print("base exception", e.__class__, "|||", e)
                            async with httpx_client_storage_lock:
                                httpx_client_storage.clear()
                            await asyncio.sleep(2)
                            await download_file_write_file(download_download_url, download_file_path)
                        await downloaded.write(f"{download_file_path},{line_nr}\n")
                        await downloaded.fsync()
                        print("complete in ", time.perf_counter() - start_time)

                    m = await async_exists_in_cache("missing_files_remote", download_url)
                    if m:
                        print("skip cache")
                        continue
                    await tg.create_task_semaphored(download(file_path, download_url, line))


if __name__ == "__main__":
    asyncio.run(main())
    print("end", handle_keyboard_interrupt.mutable["is_requested"])
