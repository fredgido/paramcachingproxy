import asyncio
from functools import wraps, partial

import renameat2


def aiowrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


aio_safe_rename = aiowrap(renameat2.rename)
