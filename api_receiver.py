import asyncio
import datetime
import pathlib
import re
import time
import urllib.parse
import uuid
from asyncio import BaseEventLoop, AbstractEventLoop
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, Union

import aiofiles
import aiofiles.os
import asyncpg
import httpx
import uvicorn
from asgiref.typing import HTTPScope
from asyncpg import Connection as APGConnection, Pool

from pg_connection_creds import connection_creds

client = httpx.AsyncClient()

CHUNK_SIZE = int(65536)

ReceiveType = Callable[[], Awaitable]
SendType = Callable[[dict], Awaitable]


@dataclass
class Connection:
    scope: HTTPScope
    receive: ReceiveType
    send: SendType
    log_info: dict


insert_statement = """INSERT INTO public.api_dump (url, "data",created_at) VALUES($1, $2, $3);"""

pool_storage = dict[AbstractEventLoop, Pool]()


class App:
    async def __call__(self, scope: HTTPScope, receive: ReceiveType, send: SendType):
        assert scope["type"] == "http"

        connection = Connection(scope, receive, send, dict())
        # print(scope)
        r = await receive()  # this is nothing
        print(r)

        query = urllib.parse.parse_qs(connection.scope["query_string"])

        url_params: Optional[list[bytes]] = query.get(b"url")
        if not url_params:
            print(query, "failed")
            return await self.return_text(connection, b"Missing query param", 400)

        url = url_params[0].decode()

        current_loop = asyncio.get_running_loop()
        pool = pool_storage.get(current_loop)
        if not pool:
            pool = await asyncpg.create_pool(min_size=10, max_size=100, **connection_creds)
            pool_storage[current_loop] = pool

        async with pool.acquire() as db_con:
            db_con: APGConnection
            values = await db_con.executemany(insert_statement, [(url, r["body"], datetime.datetime.utcnow())])
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
    uvicorn.run(App, port=1024, log_level="info", loop="uvloop", timeout_keep_alive=70, use_colors=True)
