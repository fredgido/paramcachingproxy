import asyncio

import asyncpg
from asyncpg import Connection, Pool

from pg_connection_creds import connection_creds


async def run():
    print(asyncio.get_running_loop())
    pool: Pool = await asyncpg.create_pool(min_size=10, max_size=100, **connection_creds)
    statement = """SELECT id, url, "data", created_at, processed_at FROM public.api_dump_to_recover order by id asc limit 100000"""
    async with pool.acquire() as db_con:
        db_con: Connection
        values = await db_con.fetch(statement)
        #print(values)

    await pool.close()

    for (id, url, data, created_at, processed_at) in values:
        open(f"salvage/{id}.json", "wb").write(data)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(run())
