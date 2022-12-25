import asyncio
import pathlib
from typing import Optional

import aiofiles
import httpx
import uvicorn
import re
import urllib.parse

from asgiref.typing import HTTPScope

client = httpx.AsyncClient()


twitter_media_path = pathlib.Path("twitter_media")
twitter_media_path.mkdir(exist_ok=True)

twitter_url_regex_str = r"\.twimg\.com\/(?P<type>tweet_video_thumb|media)/(?P<name>[a-zA-Z\d_-]*)(?:\?format=|.)(?P<extension>[a-zA-Z]{3,4})"
twitter_url_regex = re.compile(twitter_url_regex_str)


def twitter_url_to_orig(twitter_url: str):
    match = twitter_url_regex.search(twitter_url)
    if not match:
        None, None, None

    capture_groups = match.groupdict()
    return capture_groups["type"], capture_groups["name"], capture_groups["extension"]


ext_content_type = {
    "gif": b"image/gif",
    "jpeg": b"image/jpeg",
    "jpg": b"image/jpeg",
    "png": b"image/png",
}

CHUNK_SIZE = int(65536)


class App:
    async def __call__(self, scope: HTTPScope, receive, send):
        assert scope["type"] == "http"

        r = await receive()  # this is nothing

        if scope["path"] != "/twitter_proxy":
            print(scope["path"], "failed")
            return await self.return_error(send, b"Invalid Path")

        query = urllib.parse.parse_qs(scope["query_string"])

        url: Optional[list[bytes]] = query.get(b"url")
        if not url:
            print(query, "failed")
            return await self.return_error(send, b"Missing query param")

        url_type, name, extension = twitter_url_to_orig(url[0].decode())

        if not name:
            print(query, "failed")
            return await self.return_error(send, b"Not Found")

        file_name = f"{name}.{extension}"
        file_path = twitter_media_path / file_name
        if file_path.exists():
            await self.send_file(extension, file_path, send)
            return

        download_url = f"https://pbs.twimg.com/{url_type}/{name}?format={extension}&name=orig"
        print(download_url)
        await self.download_file_write_file_send_file(download_url, file_path, send)
        return

    @staticmethod
    async def send_file(extension, file_path, send):
        header_send_future = send(
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
            while True:
                if not chunk:
                    break
                chunk_future = aio_file.read(CHUNK_SIZE)
                send_future = send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
                chunk, send_result = await asyncio.gather(chunk_future, send_future)
                if not chunk:
                    break
        await send({"type": "http.response.body", "body": b""})

    @staticmethod
    async def return_error(send, error_message: bytes):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"text/plain"],
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": error_message,
            }
        )
        return

    @staticmethod
    async def download_file_write_file_send_file(download_url, file_path, send):
        async with aiofiles.open(file_path, "wb") as aio_file:
            async with client.stream("GET", download_url) as response:
                response_iterator = response.aiter_bytes(chunk_size=CHUNK_SIZE)

                async def wrap_read_response_stop_iter(future):
                    try:
                        return await future
                    except StopAsyncIteration:
                        return None

                header_send_future = send(
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
                while True:
                    if not read_chunk:
                        break
                    send_future = send(
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
                    if not read_chunk:
                        break

                await send({"type": "http.response.body", "body": b""})


if __name__ == "__main__":
    uvicorn.run(App, port=5000, log_level="info", loop="uvloop")
