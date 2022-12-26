import asyncio
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


class App:
    async def __call__(self, scope: HTTPScope, receive: ReceiveType, send: SendType):
        assert scope["type"] == "http"


        connection = Connection(scope, receive, send, dict())
        print(scope)
        r = await receive()  # this is nothing
        print(r)
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
