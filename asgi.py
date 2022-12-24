import pathlib
from typing import Optional

import aiofiles
import httpx
import uvicorn
import re
import urllib.parse

from asgiref.typing import HTTPScope

twitter_media_path = pathlib.Path("twitter_media")
twitter_media_path.mkdir(exist_ok=True)

image_name_url_regex_str = r"\/media\/(?P<name>[a-zA-Z\d]*)\.(?P<extension>[a-z]*)"

image_name_url_regex = re.compile(image_name_url_regex_str)

client = httpx.AsyncClient()

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


class App:
    async def __call__(self, scope: HTTPScope, receive, send):
        assert scope["type"] == "http"

        r = await receive()

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
            await send(
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
                while True:
                    chunk = await aio_file.read(65534)
                    if not chunk:
                        break  # End of file
                    await send(
                        {
                            "type": "http.response.body",
                            "body": chunk,
                            "more_body": bool(chunk),
                        }
                    )
                await send({"type": "http.response.body", "body": b""})

        download_url = f"https://pbs.twimg.com/{url_type}/{name}?format={extension}&name=orig"
        print(download_url)
        async with client.stream("GET", download_url) as response:
            await send(
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
            async for chunk in response.aiter_bytes(chunk_size=65534):
                await send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": bool(chunk),
                    }
                )
            await send({"type": "http.response.body", "body": b""})
        return

    async def return_error(self, send, error_message: bytes):
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


if __name__ == "__main__":
    if __name__ == "__main__":
        uvicorn.run(App, port=5000, log_level="info", loop="uvloop")
