import asyncio
import time

import uvloop


async def test_co():
    print("start sleep corutine")
    await asyncio.sleep(5)
    print("slept 1")
    await asyncio.sleep(5)
    print("slept 2")
    return


class TaskGroupWithSemaphore(asyncio.TaskGroup):
    def __init__(self, max_concurrent_tasks: int = 60):
        super().__init__()
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def sem_task(self, coro):
        async with self.semaphore:
            return await coro

    def create_task(self, coro, *, name=None, context=None):
        return super().create_task(self.sem_task(coro), name=None, context=None)

    async def create_task_semaphored(self, coro, *, name=None, context=None):
        async with self.semaphore:
            return super().create_task(self.sem_task(coro), name=None, context=None)


# async def main():
#     async with TaskGroupWithSemaphore(2) as tg:
#         task1 = tg.create_task(test_co())
#         task2 = tg.create_task(test_co())
#         await asyncio.sleep(1)
#         print("submiting task3")
#         start = time.perf_counter()
#         task3 = await tg.create_task_semaphored(test_co())
#         print("finished sumbmiting task 3", time.perf_counter() - start)
#     print("Both tasks have completed now.")


async def main():
    async with TaskGroupWithSemaphore(2) as tg:
        task1 = await tg.create_task_semaphored(test_co())
        task2 = await tg.create_task_semaphored(test_co())
        await asyncio.sleep(1)
        print("submiting task3")
        start = time.perf_counter()
        task3 = await tg.create_task_semaphored(test_co())
        print("finished sumbmiting task 3", time.perf_counter() - start)
    print("Both tasks have completed now.")

if __name__ == "__main__":
    start = time.perf_counter()
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner0:
        runner0.run(main())
    print(time.perf_counter() - start)
