import time

open("test_line_iter_file", "w").write("\n".join(str(i) for i in range(100000)))
start = time.perf_counter()
with open("test_line_iter_file", "r", buffering=4192 * 16) as f:
    last_line_time = time.perf_counter()
    for line in f:
        # print("line_time", time.perf_counter() - last_line_time)
        last_line_time = time.perf_counter()
        # print(line, end="")

print("end_time", time.perf_counter() - start)