import asyncio
import pathlib
import threading

caches = {}
all_lock = threading.Lock()
all_lock_async = asyncio.Lock()


def exists_in_cache(cache_file, key: str):
    setup(cache_file)
    return key in caches[cache_file]["current"]

def add_to_cache(cache_file, key: str):
    setup(cache_file)
    if key not in caches[cache_file]["current"]:
        with get_correct_lock(caches[cache_file]["lock"],caches[cache_file]["async_lock"]):
            caches[cache_file]["file"].write(f"{key}\n")
            caches[cache_file]["current"].add(key)


def setup(cache_file):
    if not cache_file in caches:
        with get_correct_lock(all_lock,all_lock_async):
            if not pathlib.Path(cache_file).exists():
                open(cache_file, "w").close()
            current_keys = set(line for line in open(cache_file, "r"))
            caches[cache_file] = dict(
                lock=threading.Lock(),
                async_lock=asyncio.Lock(),
                file=open(cache_file, "a"),
                current=current_keys,
                queue=set(),
            )


async def async_exists_in_cache(cache_file, key: str):
    await async_setup(cache_file)
    return key in caches[cache_file]["current"]


async def async_add_to_cache(cache_file, key: str):
    await async_setup(cache_file)
    if key not in caches[cache_file]["current"]:
        async with caches[cache_file]["async_lock"]:
            caches[cache_file]["file"].write(f"{key}\n")
            caches[cache_file]["file"].flush()
            caches[cache_file]["current"].add(key)


async def async_setup(cache_file):
    if not cache_file in caches:
        async  with all_lock_async:
            if not pathlib.Path(cache_file).exists():
                open(cache_file, "w").close()
            current_keys = set(line.strip("\n") for line in open(cache_file, "r"))
            caches[cache_file] = dict(
                lock=threading.Lock(),
                async_lock=asyncio.Lock(),
                file=open(cache_file, "a"),
                current=current_keys,
                queue=set(),
            )



# def sync(cache_file):
#     if not cache_file in caches:
#         return
#
#     caches[cache_file]


