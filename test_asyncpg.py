import asyncio
import datetime
import json

import asyncpg
from asyncpg import Connection

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


async def run():
    db_con: Connection = await asyncpg.connect(**connection_creds)
    values = await db_con.fetch(
        "SELECT * FROM public.api_dump WHERE id = $1",
        2794617,
    )
    print(values[0])
    i = values[0][2][:-2].decode("utf-16")
    print(json.loads(values[0][2].decode("utf-16")))
    # statement = """INSERT INTO public.api_dump (url, "data",created_at) VALUES($1, $2, $3);"""
    # values = await db_con.executemany(statement, [("localhost", b"B", datetime.datetime.utcnow())])
    # print(values)

    await db_con.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(run())
