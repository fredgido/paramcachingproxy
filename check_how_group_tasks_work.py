import asyncio
import time

import uvloop


async def test_co():
    print("start sleep corutine")
    await asyncio.sleep(1)
    print("slept 1")
    await asyncio.sleep(1)
    print("slept 2")
    return


async def main():
    async with asyncio.TaskGroup() as tg:
        task1 = tg.create_task(test_co())
        print("starting to sleep 5 seconds")
        await asyncio.sleep(5)
        print("finished to sleep 5 seconds")
        task2 = tg.create_task(test_co())
    print("Both tasks have completed now.")


if __name__ == "__main__":
    start = time.perf_counter()
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner0:
        runner0.run(main())
    print(time.perf_counter() - start)
