import asyncio
import datetime
import os
import time
import urllib.parse
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable
from dateutil.parser import parse

import asyncpg
import orjson
import uvicorn
from asgiref.typing import HTTPScope
from asyncpg import Connection as APGConnection, Pool

from pg_connection_creds import connection_creds

select_statement = """
select json_agg(post_and_assets)
from (
SELECT json_build_object('post',row_to_json(p),'assets',json_agg(row_to_json(a)),'user',json_agg(row_to_json(u))) as post_and_assets
FROM public.post p
left join public.asset a on p.id = a.post_id 
left join public."user" u on p.user_id = u.id 
where p.id = any($1::int8[])
group by p.id 
having count(a.id) >0
) as post_assets_json_query
"""

select_statement_recent = """
select json_agg(post_and_assets)
from (
SELECT json_build_object('post',row_to_json(p),'assets',json_agg(row_to_json(a))) as post_and_assets
FROM public.post p
left join public.asset a on p.id =a.post_id 
group by p.id 
order by p.processed_at desc
) as post_assets_json_query
limit 100
"""

select_recent_assets = """
select post_id
FROM public.asset
order by processed_at  desc
limit 1000
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

        body = b""
        data = await receive()
        body += data["body"]
        while data["more_body"]:
            data = await receive()
            # print(data, url)
            body += data["body"]

        connection = Connection(scope, receive, send, body, dict())

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
        query: dict[str, list[str]] = urllib.parse.parse_qs(connection.scope["query_string"].decode())  # noqa
        post_object: dict[str, str] = orjson.loads(connection.request_body) if connection.request_body else {}

        squashed_query_object: dict[str, str] = {k: v[0] for k, v in query.items()}

        filter_object: dict[str, str] = {**squashed_query_object, **post_object}

        strtobool = lambda x: bool(x and x[0].lower() in ("y", "t", "o", "1"))
        int_id_comma = lambda x: ([int(x_) for x_ in x.split(",")] if isinstance(x,str) else [x] )
        str_comma = lambda x: [str(x_) for x_ in x.split(",")]
        parse_timestamp = lambda x: parse(x)

        def solve_range(range_str):
            parts = range_str.split("..")
            start = None if not parts[0] else int(parts[0])
            end = None if not parts[1] else int(parts[1])
            return start, end

        sql_conditions_base = {
            "search[id]": ("p.id = any($N::int8[])", int_id_comma),
            "search[user]": ("p.user = any($N::int8[])", int_id_comma),
            "search[user_id]": ("p.user = any($N::int8[])", int_id_comma),
            "search[user_handle]": ("u.screen_name = any($N::text[])", str_comma),
            "search[body]": ("p.full_text ilike $N ", lambda x: f"%{x}%"),
            "search[hashtags]": ("p.user = any($N::int8[])", str_comma),
            "search[mentions]": ("p.user = any($N::int8[])", int_id_comma),
            "search[mentions_handle]": ("p.user = any($N::int8[])", str_comma),
            "search[urls]": ("p.user = any($N::int8[])", str_comma),
            "search[start_timestamp]": ("p.user = any($N::int8[])", parse_timestamp),
            "search[end_timestamp]": ("p.user = any($N::int8[])", parse_timestamp),
            "search[retweets]": ("p.user = any($N::int8[])", solve_range),
            "search[favorites]": ("p.user = any($N::int8[])", solve_range),
            "search[replies]": ("p.user = any($N::int8[])", solve_range),
            "search[views]": ("p.user = any($N::int8[])", solve_range),
            "search[is_quote_tweet]": ("p.user = any($N::int8[])", strtobool),
            "search[is_retweet]": ("p.is_retweet = any($N::int8[])", strtobool),
            "search[has_images]": ("p.user = any($N::int8[])", strtobool),
            "search[has_videos]": ("p.user = any($N::int8[])", strtobool),
        }
        sql_conditions = ""
        sql_params = []
        """
        search[id] search[user] search[body] search[hashtags] search[mentions] search[urls] search[timestamp] search[retweets] search[favorites] search[replies] search[views] search[is_quote_tweet] search[is_retweet] search[has_images] search[has_videos]
        """

        for sql_filter_key, sql_filter_value in filter_object.items():
            condition_str, arg_parser = sql_conditions_base[sql_filter_key]
            sql_conditions += " AND " + condition_str.replace("$N", f"${len(sql_params)+1}")
            sql_params.append(arg_parser(sql_filter_value))

        base_query = """
SELECT p.id
FROM public.post p
left join public.asset a on p.id =a.post_id 
where true
"""
        if "search[user_handle]" in sql_conditions_base or "search[mentions_handle]" in sql_conditions_base:
            base_query = "\n".join(
                base_query.split("\n")[0:3]
                + ['left join public."user" u on p.user_id = u.id ']
                + base_query.split("\n")[3:]
            )

        built_query = base_query + sql_conditions + " order by id desc" + " limit 1000 "
        pool = await get_pg_connection_pool()
        async with pool.acquire() as db_con:
            db_con: APGConnection
            values = await db_con.fetch(built_query, *sql_params)
            print(values)
            values = await db_con.fetch(select_statement, [value.get("id") for value in values])

            if values and values[0][0]:
                return await cls.return_json(connection, values[0][0].encode())
            else:
                return await cls.return_json(connection, {"error": "not found"}, status_code=404)

        if not query:
            print(query, "failed")
            return await cls.return_json(connection, {"error": "Missing query param"})

        if "id" in query:
            ids = query.get("id")
            ids = [int(post_id) for post_id in (",".join(ids)).decode().split(",")]

            pool = await get_pg_connection_pool()
            async with pool.acquire() as db_con:
                db_con: APGConnection
                values = await db_con.fetch(select_statement, [ids])

            if values and values[0][0]:
                return await cls.return_json(connection, values[0][0].encode())
            else:
                return await cls.return_json(connection, {"error": "not found"}, status_code=404)

        if "recent" in query:
            pool = await get_pg_connection_pool()
            async with pool.acquire() as db_con:
                db_con: APGConnection
                post_ids = await db_con.fetch(select_recent_assets)
                values = await db_con.fetch(select_statement, [post[0] for post in post_ids])

            if values and values[0][0]:
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
                    [b"access-control-allow-origin", b"*"],
                    [b"access-control-request-method", b"GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"],
                    [b"access-control-allow-headers", b"*"],
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
        port=int(os.getenv("PORT")) if os.getenv("PORT") else 7037,
        log_level="info",
        # log_level="critical",
        loop="uvloop",
        timeout_keep_alive=70,
        use_colors=True,
        # workers=UVICORN_WORKERS,
    )
