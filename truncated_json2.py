import asyncio
import datetime

import asyncpg
import orjson
from asyncpg import Connection, Pool

from pg_connection_creds import connection_creds


import half_json.json_util

def new(e):
    assert isinstance(e, ValueError)

    message = e.args[0]
    idx = message.rindex(':')
    errmsg, left = message[:idx], message[idx + 1:]
    import re
    numbers = re.compile(r'\d+').findall(left)
    parser = e.__dict__.get("parser", "")

    from half_json.json_util import errors
    result = {
        "parsers": e.__dict__.get("parsers", []),
        "error": errors.get_decode_error(parser, errmsg),
        "lineno": int(numbers[0]),
        "colno": int(numbers[1]),
    }

    if len(numbers) == 3:
        result["pos"] = int(numbers[2])

    if len(numbers) > 3:
        result["endlineno"] = int(numbers[2])
        result["endcolno"] = int(numbers[3])
        result["pos"] = int(numbers[4])
        result["end"] = int(numbers[5])
    return result


half_json.json_util.errmsg_inv = new



data = open(f"salvage/{2794252}.json","rb").read()


bad_encoding = False
try:
    clean_data = data.decode("utf-8")
except Exception:
    bad_encoding = True
    # clean utf-8
    if b"$" in data:
        print("fail")
    clean_data = data.replace(b"?", b"$")
    clean_data = clean_data.decode("utf-8", "replace")
    indexes = [i for i, ltr in enumerate(clean_data) if ltr == "?"]
    print(indexes)
print(clean_data)
mpa = dict.fromkeys(range(32))
clean_data= clean_data.translate(mpa)

from half_json.core import JSONFixer

f = JSONFixer()
result = f.fix(clean_data.encode("ascii","replace").decode().replace("\\??",""))
print(result)


