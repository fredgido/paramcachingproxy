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
from typing import Optional, Callable, Awaitable, Union

import aiofiles
import aiofiles.os
import httpx
import uvicorn
from asgiref.typing import HTTPScope

from aio_safe_rename import aio_safe_rename

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
        return None, None, None, None, None

    capture_groups = match.groupdict()
    return (
        capture_groups["subdomain"],
        capture_groups["type"],
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
    file_path_placeholder = str(file_path) + str(uuid.uuid4())
    async with aiofiles.open(file_path_placeholder, "wb") as aio_file:
        async with client.stream("GET", download_url) as response:  # todo save headers
            response_iterator = response.aiter_bytes(chunk_size=CHUNK_SIZE * 4)

            async def wrap_read_response_stop_iter(future) -> Optional[bytes]:
                try:
                    return await future
                except StopAsyncIteration:
                    return None

            response_future = wrap_read_response_stop_iter(response_iterator.__anext__())

            read_chunk = await response_future
            while True:
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

    try:
        await aio_safe_rename(file_path_placeholder, file_path, replace=False, loop= asyncio.get_running_loop())
    except FileExistsError as e:
        print("failed renameat2")
    # await aiofiles.os.rename(file_path_placeholder, file_path)


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    with open(f"temp/downloaded_{datetime.datetime.utcnow().isoformat()}.txt", "a") as downloaded:
        for line in open("temp/asset_urls2.csv", "r"):
            line = line.strip()

            subdomain, url_type, name, extension = twitter_url_to_orig(line)
            if not name:
                (
                    subdomain,
                    type,
                    name,
                    extension,
                    resolution,
                ) = twitter_video_url_to_orig(line)
                if not name:
                    print(line, "failed")
                    continue
                print("video")
                continue
            if type == "video":
                print("skip video")
            if "tweet_video_thumb" in line:
                print("skip thumb")

            file_name = f"{name}.{extension}"
            file_path = twitter_media_path / file_name
            if file_path.exists():
                print("exists", line)
                continue
            print("doesn't exist", line)

            if subdomain == "video":
                download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}.{extension}"
            else:
                download_url = f"https://{subdomain}.twimg.com/{url_type}/{name}?format={extension}&name=orig"


            loop.run_until_complete(download_file_write_file(download_url, file_path))
            print("complete")
            downloaded.write(f"{file_path},{line}\n")
            downloaded.flush()
