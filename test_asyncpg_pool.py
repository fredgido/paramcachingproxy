import asyncio
import datetime
from asyncio.unix_events import _UnixSelectorEventLoop

import asyncpg
from asyncpg import Connection, Pool

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
_UnixSelectorEventLoop

async def run():
    print(asyncio.get_running_loop())
    pool: Pool = await asyncpg.create_pool(min_size=10, max_size=100, **connection_creds)
    statement = """INSERT INTO public.api_dump (url, "data",created_at) VALUES($1, $2, $3);"""
    async with pool.acquire() as db_con:
        db_con: Connection
        values = await db_con.executemany(statement, [("localhost", b"B", datetime.datetime.utcnow())])
        print(values)

    await pool.close()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(run())
