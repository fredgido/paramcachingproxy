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

"""
create table public.api_dump (
	id serial primary key,
	url text not null,
	data bytea not null,
	created_at timestamptz not null,
	processed_at timestamptz null
);
"""
insert_statement = """INSERT INTO public.api_dump (url, "data",created_at) VALUES($1, $2, $3);"""


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


class App:
    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"

        query = urllib.parse.parse_qs(scope["query_string"])

        url_params: Optional[list[bytes]] = query.get(b"url")
        url = url_params[0].decode()

        body = b""
        data = await receive()
        #print(data, url)
        body += data["body"]
        while data["more_body"]:
            data = await receive()
            #print(data, url)
            body += data["body"]
        connection = Connection(scope, receive, send, body, dict())
        # print(scope)
        #print(connection.request_body)
        try:
            if body:
                t = orjson.loads(body)
                #print(t, url)
        except Exception:
            print("fail decode", url)

        query = urllib.parse.parse_qs(connection.scope["query_string"])

        url_params: Optional[list[bytes]] = query.get(b"url")
        if not url_params:
            print(query, "failed")
            return await self.return_text(connection, b"Missing query param", 400)

        url = url_params[0].decode()

        pool = await get_pg_connection_pool()
        async with pool.acquire() as db_con:
            db_con: APGConnection
            values = await db_con.executemany(
                insert_statement, [(url, connection.request_body, datetime.datetime.utcnow())]
            )
        return await self.return_text(connection, b"OK", 200)

    @staticmethod
    async def return_text(connection: Connection, error_message: bytes, status_code: int = 200):
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
                "body": error_message,
            }
        )
        return


if __name__ == "__main__":
    uvicorn.run(
        "api_receiver:App",
        port=1024,
        log_level="info",
        # log_level="critical",
        loop="uvloop",
        timeout_keep_alive=70,
        use_colors=True,
        # workers=UVICORN_WORKERS,
    )
