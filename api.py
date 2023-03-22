import asyncio
import datetime
import time
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
select json_agg(post_and_assets)
from (
SELECT json_build_object('post',row_to_json(p),'assets',json_agg(row_to_json(a))) as post_and_assets
FROM public.post p
left join public.asset a on p.id =a.post_id 
where p.id = any($1::int8[])
group by p.id 
having count(a.id) >0
) as post_assets_json_query
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

        # routes
        if scope["path"].startswith("/tweet"):
            return await self.tweet_handler(connection)
        if scope["path"].startswith("/user"):
            return await self.listing_handler(connection)
        # print(scope["path"], "failed")
        connection.log_info["bad_path"] = True
        return await self.return_text(connection, b"Invalid URL Path", 403)

    @classmethod
    async def tweet_handler(cls, connection: Connection):
        connection.log_info["chunks_size"] = 0
        connection.log_info["chunks_times"] = []
        connection.log_info["start_time"] = time.perf_counter()
        query: dict[bytes, list[bytes]] = urllib.parse.parse_qs(connection.scope["query_string"])

        if not query:
            print(query, "failed")
            return await cls.return_json(connection, {"error": "Missing query param"})

        if b"id" in query:
            ids = query.get(b"id")
            ids = [int(post_id) for post_id in (b",".join(ids)).decode().split(",")]

            pool = await get_pg_connection_pool()
            async with pool.acquire() as db_con:
                db_con: APGConnection

                def encoder(x):
                    return x

                def decoder(x):
                    return x

                await db_con.set_type_codec("text", schema="pg_catalog", encoder=encoder, decoder=decoder, format="binary")
                values = await db_con.fetch(select_statement, [ids])

            if values:
                return await cls.return_json(connection, values[0][0].encode())
            else:
                return await cls.return_json(connection, {"error": "not found"}, status_code=404)
        return await cls.return_json(connection, {"error": "unknown query param"})

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

    @staticmethod
    async def return_json(connection: Connection, message: dict | bytes, status_code: int = 200):
        await connection.send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    [b"content-type", b"application/json"],
                ],
            }
        )
        await connection.send(
            {
                "type": "http.response.body",
                "body": orjson.dumps(message) if isinstance(message, dict) else message,
            }
        )
        return


if __name__ == "__main__":
    uvicorn.run(
        "api:App",
        host="0.0.0.0",
        port=7037,
        log_level="info",
        # log_level="critical",
        loop="uvloop",
        timeout_keep_alive=70,
        use_colors=True,
        # workers=UVICORN_WORKERS,
    )
