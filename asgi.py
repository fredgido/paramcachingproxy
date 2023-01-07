import asyncio
import os
import pathlib
import re
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, Union

import aiofiles
import aiofiles.os
import httpx
import uvicorn
from asgiref.typing import HTTPScope

CoroutineFunction = Callable[[Union[dict, type(None)]], Awaitable]

client = httpx.AsyncClient()

path_settings = os.getenv("MEDIA_FOLDER", ".")
twitter_media_path = pathlib.Path(f"{path_settings}/twitter_media")
twitter_media_path.mkdir(exist_ok=True)

twitter_url_regex_str = r"(?P<subdomain>[^.\/]*)\.twimg\.com\/(?P<type>tweet_video_thumb|media|tweet_video)/(?P<name>[a-zA-Z\d_-]*)(?:\?format=|\.)(?P<extension>[a-zA-Z0-9]{3,4})"
twitter_url_regex = re.compile(twitter_url_regex_str)


def twitter_url_to_orig(twitter_url: str):
    match = twitter_url_regex.search(twitter_url)
    if not match:
        None, None, None, None

    capture_groups = match.groupdict()
    return capture_groups["subdomain"], capture_groups["type"], capture_groups["name"], capture_groups["extension"]


ext_content_type = {
    "gif": b"image/gif",
    "jpeg": b"image/jpeg",
    "jpg": b"image/jpeg",
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

        # r = await receive()  # this is nothing

        connection = Connection(scope, receive, send, dict())

        if scope["path"] == "/twitter_proxy":
            return await self.twitter_proxy_handler(connection)
        print(scope["path"], "failed")
        return await self.return_error(connection, b"Invalid URL Path")

    async def twitter_proxy_handler(self, connection: Connection):
        connection.log_info["chunks_size"] = 0
        connection.log_info["chunks_times"] = []
        connection.log_info["start_time"] = time.perf_counter()
        query = urllib.parse.parse_qs(connection.scope["query_string"])

        url_params: Optional[list[bytes]] = query.get(b"url")
        if not url_params:
            print(query, "failed")
            return await self.return_error(connection, b"Missing query param")

        url = url_params[0].decode()
        connection.log_info["url"] = url
        subdomain, url_type, name, extension = twitter_url_to_orig(url)
        connection.log_info["url_type"] = url_type
        connection.log_info["name"] = name
        connection.log_info["extension"] = extension

        if not name:
            print(query, "failed")
            return await self.return_error(connection, b"Not Found")

        file_name = f"{name}.{extension}"
        file_path = twitter_media_path / file_name
        if file_path.exists():
            connection.log_info["file_exists"] = True
            await self.send_file(connection, extension, file_path)
            connection.log_info["end_time"] = time.perf_counter() - connection.log_info["start_time"]
            print(connection.log_info)
            return

        if subdomain == "video":
            download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}.{extension}"
        else:
            download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}?format={extension}&name=orig"
        print(download_url)
        await self.download_file_write_file_send_file(connection, download_url, file_path)
        connection.log_info["file_exists"] = False
        connection.log_info["end_time"] = time.perf_counter() - connection.log_info["start_time"]
        print(connection.log_info)
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
    async def return_error(connection, error_message: bytes):
        await connection.send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    #[b"content-type", b"text/plain"],
                ],
            }
        )
        await connection.send(
            {
                "type": "http.response.body",
                "body": error_message,
            }
        )
        return

    async def download_file_write_file_send_file(
        self, connection: Connection, download_url: str, file_path: str | pathlib.Path
    ):
        file_path_placeholder = str(file_path) + str(uuid.uuid4())
        connection.log_info["download_start_time"] = time.perf_counter() - connection.log_info["start_time"]
        async with aiofiles.open(file_path_placeholder, "wb") as aio_file:
            async with client.stream("GET", download_url) as response:  # todo save headers
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
        await aiofiles.os.rename(file_path_placeholder, file_path)
        connection.log_info["rename_time"] = time.perf_counter() - connection.log_info["start_time"]


if __name__ == "__main__":
    uvicorn.run(App, port=5000, log_level="info", loop="uvloop", timeout_keep_alive=70, use_colors=True)
