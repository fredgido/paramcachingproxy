import asyncio
import datetime
import urllib.parse
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable

import asyncpg
import orjson
import uvicorn
from asgiref.typing import HTTPScope
from asyncpg import Connection as APGConnection, Pool

from pg_connection_creds import connection_creds

select_statement = """
SELECT row_to_json(p), array_agg(row_to_json(a))
FROM public.post p
left join public.asset a on p.id =a.post_id 
where p.id = any($1::int[])
group by p.id 
having count(a.id) >0
"""


@dataclass
class Connection:
    scope: HTTPScope
    receive: Callable[[], Awaitable]
    send: Callable[[dict], Awaitable]
    request_body: bytes
    log_info: dict


pool_storage = dict[AbstractEventLoop, Pool]()
pool_storage_lock = asyncio.Lock()
UVICORN_WORKERS = 4
POOL_MIN_WORKERS = int(10 / float(UVICORN_WORKERS))
POOL_MAX_WORKERS = int(100 / float(UVICORN_WORKERS)) - 1


async def get_pg_connection_pool():
    current_loop = asyncio.get_running_loop()
    pool = pool_storage.get(current_loop)
    if not pool:
        async with pool_storage_lock:
            pool = pool_storage.get(current_loop)
            if not pool:
                pool = await asyncpg.create_pool(
                    min_size=POOL_MIN_WORKERS, max_size=POOL_MAX_WORKERS, **connection_creds
                )
                pool_storage[current_loop] = pool
    return pool

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
        #print(scope["path"], "failed")
        connection.log_info["bad_path"] = True
        logs_file.write(json.dumps(connection.log_info)+ "\n")
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
        #print(connection.log_info)
        connection.log_info["twitter_downloader"] = True
        logs_file.write(json.dumps(connection.log_info)+ "\n")
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
        connection.log_info["url_type"] = url_type
        connection.log_info["name"] = name
        connection.log_info["extension"] = extension

        if not name:
            print(query, "failed")
            return await self.return_text(connection, b"Not Found")

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
        #print(connection.log_info)
        connection.log_info["twitter_proxy"] = True
        logs_file.write(json.dumps(connection.log_info)+ "\n")
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

        await aiofiles.os.rename(file_path_placeholder, file_path)
        connection.log_info["rename_time"] = time.perf_counter() - connection.log_info["start_time"]

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
    uvicorn.run(
        "api_receiver:App",
        host="0.0.0.0",
        port=7022,
        log_level="info",
        # log_level="critical",
        loop="uvloop",
        timeout_keep_alive=70,
        use_colors=True,
        # workers=UVICORN_WORKERS,
    )
