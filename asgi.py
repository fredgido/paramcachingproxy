import asyncio
import datetime
import json
import os
import pathlib
import re
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from functools import wraps, partial
from typing import Optional, Callable, Awaitable, Union

import aiofiles
import aiofiles.os
import httpx
import renameat2
import uvicorn
from asgiref.typing import HTTPScope

CoroutineFunction = Callable[[Union[dict, type(None)]], Awaitable]

client = httpx.AsyncClient(timeout=61)

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
# todo aiofile instead of aiofiles


def aiowrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


aio_safe_rename = aiowrap(renameat2.rename)


def twitter_url_to_orig(twitter_url: str):
    match = twitter_url_regex.search(twitter_url)
    if not match:
        return None, None, None, None

    capture_groups = match.groupdict()
    return capture_groups["subdomain"], capture_groups["type"], capture_groups["name"], capture_groups["extension"]


ext_content_type = {
    "gif": b"image/gif",
    "jpeg": b"image/jpeg",
    "jpg": b"image/jpeg",
    "webp": b"image/jpeg",
    "png": b"image/png",
    "mp4": b"video/mp4",
}

CHUNK_SIZE = int(65536)

ReceiveType = Callable[[], Awaitable]
SendType = Callable[[dict], Awaitable]


@dataclass
class Connection:
    scope: HTTPScope
    receive: ReceiveType
    send: SendType
    log_info: dict


class App:
    async def __call__(self, scope: HTTPScope, receive: ReceiveType, send: SendType):
        assert scope["type"] == "http"

        if scope["method"] == "OPTIONS":
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        [b"access-control-allow-origin", b"*"],
                        [b"access-control-request-method", b"GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"],
                        [b"access-control-allow-headers", b"*"],
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": "",
                }
            )
            print("OPTIONS", scope["query_string"])
            return
        # r = await receive()  # this is nothing

        connection = Connection(scope, receive, send, dict())

        if scope["path"] == "/twitter_proxy":
            return await self.twitter_proxy_handler(connection)
        if scope["path"] == "/twitter_downloader":
            return await self.twitter_downloader_handler(connection)
        # print(scope["path"], "failed")
        connection.log_info["bad_path"] = True
        logs_file.write(json.dumps(connection.log_info) + "\n")
        return await self.return_text(connection, b"Invalid URL Path", 403)

    async def twitter_downloader_handler(self, connection: Connection):
        connection.log_info["passive_downloader"] = True
        connection.log_info["chunks_size"] = 0
        connection.log_info["chunks_times"] = []
        connection.log_info["start_time"] = time.perf_counter()
        query = urllib.parse.parse_qs(connection.scope["query_string"])

        url_params: Optional[list[bytes]] = query.get(b"url")
        if not url_params:
            print(query, "failed")
            return await self.return_text(connection, b"Missing query param", 400)

        url = url_params[0].decode()
        connection.log_info["url"] = url
        subdomain, url_type, name, extension = twitter_url_to_orig(url)
        connection.log_info["url_type"] = url_type
        connection.log_info["name"] = name
        connection.log_info["extension"] = extension

        if not name:
            print(query, "failed")
            return await self.return_text(connection, b"Not Found", 400)

        await self.return_text(connection, b"OK", 200)

        file_name = f"{name}.{extension}"
        file_path = twitter_media_path / file_name
        if file_path.exists():
            connection.log_info["file_exists"] = True
            connection.log_info["end_time"] = time.perf_counter() - connection.log_info["start_time"]
            print(connection.log_info)
            return

        if subdomain == "video":
            download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}.{extension}"
        else:
            download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}?format={extension}&name=orig"
        print(download_url)
        await self.download_file_write_file(connection, download_url, file_path)
        connection.log_info["file_exists"] = False
        connection.log_info["end_time"] = time.perf_counter() - connection.log_info["start_time"]
        # print(connection.log_info)
        connection.log_info["twitter_downloader"] = True
        logs_file.write(json.dumps(connection.log_info) + "\n")
        return

    async def twitter_proxy_handler(self, connection: Connection):
        connection.log_info["chunks_size"] = 0
        connection.log_info["chunks_times"] = []
        connection.log_info["start_time"] = time.perf_counter()
        query = urllib.parse.parse_qs(connection.scope["query_string"])

        url_params: Optional[list[bytes]] = query.get(b"url")
        if not url_params:
            print(query, "failed")
            return await self.return_text(connection, b"Missing query param")

        url = url_params[0].decode()
        connection.log_info["url"] = url
        subdomain, url_type, name, extension = twitter_url_to_orig(url)
        if extension == "webp":
            print("webp")
            connection.log_info["webp_sample"] = True
            extensions = ["jpg", "png", "gif"]
        else:
            extensions = [extension]
        connection.log_info["url_type"] = url_type
        connection.log_info["name"] = name
        connection.log_info["extension"] = extension

        if not name:
            print(query, "failed")
            return await self.return_text(connection, b"Not Found")

        for ext in extensions:
            file_path = twitter_media_path / f"{name}.{ext}"
            if file_path.exists():
                connection.log_info["file_exists"] = True
                await self.send_file(connection, ext, file_path)
                connection.log_info["end_time"] = time.perf_counter() - connection.log_info["start_time"]
                print(connection.log_info)
                return

        for ext in extensions:
            file_path = twitter_media_path / f"{name}.{ext}"
            if subdomain == "video":
                download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}.{ext}"
            else:
                download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}?format={ext}&name=orig"
            connection.log_info["download_url"] = download_url
            if await self.download_file_write_file_send_file(connection, download_url, file_path):
                break
        connection.log_info["file_exists"] = False
        connection.log_info["end_time"] = time.perf_counter() - connection.log_info["start_time"]
        # print(connection.log_info)
        connection.log_info["twitter_proxy"] = True
        logs_file.write(json.dumps(connection.log_info) + "\n")
        return

    async def send_file(self, connection: Connection, extension: str, file_path: str | pathlib.Path):
        header_send_future = connection.send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", ext_content_type[extension]],
                    [b"cache-control", b"max-age=604800"],
                    # [b"Content-Length", response.headers.get("Content-Length").encode()],
                    [b"Connection", b"keep-alive"],
                ],
            }
        )
        async with aiofiles.open(file_path, "rb") as aio_file:
            print("return existing", file_path)
            first_chunk_future = aio_file.read(CHUNK_SIZE)
            chunk, header_send_result = await asyncio.gather(first_chunk_future, header_send_future)
            connection.log_info["header_first_chunk_time"] = time.perf_counter() - connection.log_info["start_time"]
            connection.log_info["chunks_size"] += len(chunk)
            while True:
                if not chunk:
                    break
                chunk_future = aio_file.read(CHUNK_SIZE)
                send_future = connection.send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
                chunk, send_result = await asyncio.gather(chunk_future, send_future)
                connection.log_info["chunks_times"].append(time.perf_counter() - connection.log_info["start_time"])
                connection.log_info["chunks_size"] += len(chunk)
                if not chunk:
                    break

        connection.log_info["end_packet_time"] = time.perf_counter() - connection.log_info["start_time"]
        await connection.send({"type": "http.response.body", "body": b""})

    @staticmethod
    async def return_text(connection: Connection, message: bytes, status_code: int = 200):
        await connection.send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    [b"content-type", b"text/plain"],
                ],
            }
        )
        await connection.send(
            {
                "type": "http.response.body",
                "body": message,
            }
        )
        return

    async def download_file_write_file(self, connection: Connection, download_url: str, file_path: str | pathlib.Path):
        file_path_placeholder = str(file_path) + str(uuid.uuid4())
        connection.log_info["download_start_time"] = time.perf_counter() - connection.log_info["start_time"]
        async with aiofiles.open(file_path_placeholder, "wb") as aio_file:
            async with client.stream("GET", download_url) as response:  # todo save headers
                connection.log_info["response_time"] = time.perf_counter() - connection.log_info["start_time"]
                response_iterator = response.aiter_bytes(chunk_size=CHUNK_SIZE * 4)

                async def wrap_read_response_stop_iter(future) -> Optional[bytes]:
                    try:
                        return await future
                    except StopAsyncIteration:
                        return None

                response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

                read_chunk = await response_future
                connection.log_info["header_first_chunk_time"] = time.perf_counter() - connection.log_info["start_time"]
                connection.log_info["chunks_size"] += len(read_chunk) if read_chunk else 0
                while True:
                    if not read_chunk:
                        break

                    file_write_future = aio_file.write(read_chunk)
                    response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

                    read_chunk, file_write_result = await asyncio.gather(
                        response_future,
                        file_write_future,
                    )
                    connection.log_info["chunks_times"].append(time.perf_counter() - connection.log_info["start_time"])
                    if read_chunk:
                        connection.log_info["chunks_size"] += len(read_chunk)
                    if not read_chunk:
                        break

        try:
            await aio_safe_rename(file_path_placeholder, file_path, replace=False)
        except FileExistsError as e:
            print("failed renameat2")
        # await aiofiles.os.rename(file_path_placeholder, file_path)
        connection.log_info["rename_time"] = time.perf_counter() - connection.log_info["start_time"]

    async def download_file_write_file_send_file(
        self, connection: Connection, download_url: str, file_path: str | pathlib.Path
    )-> bool:
        file_path_placeholder = str(file_path) + str(uuid.uuid4())
        connection.log_info["download_start_time"] = time.perf_counter() - connection.log_info["start_time"]
        async with client.stream("GET", download_url) as response:  # todo save headers
            print(response.__dict__)
            connection.log_info["download_status_code"] = response.status_code
            if response.status_code == 404:
                await response.aread()
                return False
            async with aiofiles.open(file_path_placeholder, "wb") as aio_file:
                connection.log_info["response_time"] = time.perf_counter() - connection.log_info["start_time"]
                response_iterator = response.aiter_bytes(chunk_size=CHUNK_SIZE)

                async def wrap_read_response_stop_iter(future) -> Optional[bytes]:
                    try:
                        return await future
                    except StopAsyncIteration:
                        return None

                header_send_future = connection.send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [
                            [b"content-type", response.headers.get("content-type", "image/jpg").encode()],
                            [b"cache-control", response.headers.get("cache-control", "max-age=604800").encode()],
                            [b"Content-Length", response.headers.get("Content-Length").encode()],
                            [b"Connection", response.headers.get("Connection", "keep-alive").encode()],
                        ],
                    }
                )
                response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

                read_chunk, header_send_result = await asyncio.gather(
                    response_future,
                    header_send_future,
                )
                connection.log_info["header_first_chunk_time"] = time.perf_counter() - connection.log_info["start_time"]
                connection.log_info["chunks_size"] += len(read_chunk) if read_chunk else 0
                while True:
                    if not read_chunk:
                        break
                    send_future = connection.send(
                        {
                            "type": "http.response.body",
                            "body": read_chunk,
                            "more_body": True,
                        }
                    )
                    file_write_future = aio_file.write(read_chunk)
                    response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

                    read_chunk, send_result, file_write_result = await asyncio.gather(
                        response_future,
                        send_future,
                        file_write_future,
                    )
                    connection.log_info["chunks_times"].append(time.perf_counter() - connection.log_info["start_time"])
                    if read_chunk:
                        connection.log_info["chunks_size"] += len(read_chunk)
                    if not read_chunk:
                        break

        await connection.send({"type": "http.response.body", "body": b""})
        try:
            await aio_safe_rename(file_path_placeholder, file_path, replace=False)
        except FileExistsError as e:
            print("failed renameat2")
        # await aiofiles.os.rename(file_path_placeholder, file_path)
        connection.log_info["rename_time"] = time.perf_counter() - connection.log_info["start_time"]
        return True

if __name__ == "__main__":
    uvicorn.run(
        App,
        host="0.0.0.0",
        port=7034,
        log_level="info",
        loop="uvloop",
        timeout_keep_alive=70,
        use_colors=True,
    )
