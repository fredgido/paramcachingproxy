from datetime import datetime

import httpx
import uvicorn
from blacksheep import Application, Response, Content, StreamedContent
import orjson as json


app = Application()
client = httpx.AsyncClient()


@app.route("/")
def home():
    return f"Hello, World! {datetime.utcnow().isoformat()}"


@app.route("/test")
async def get_movies():
    # ... do something async (example)
    movies = {"test": 1}
    a = json.dumps(movies)
    print(a)
    Response
    return Response(200, content=Content(b"text/plain", a))


import pathlib

twitter_media_path = pathlib.Path("twitter_media")
twitter_media_path.mkdir(exist_ok=True)

image_name_url_regex_str = r"\/media\/(?P<name>[a-zA-Z\d]*)\.(?P<extension>[a-z]*)"
import re

image_name_url_regex = re.compile(image_name_url_regex_str)

test_url = "https://pbs.twimg.com/media/FkqZQYrakAA5P5h.jpg:orig"

match = image_name_url_regex.search(test_url)
print(match.groupdict())


@app.route("/stream_test")
async def stream_test():

    test_url = "https://pbs.twimg.com/media/FkqZQYrakAA5P5h.jpg:orig"

    match = image_name_url_regex.search(test_url)

    if not match:
        print(test_url, "failed")
        return Response(200, content=Content(b"text/plain", b"Not Found"))

    async def provider():
        async with client.stream("GET", test_url) as response:
            async for chunk in response.aiter_bytes(chunk_size=65534):
                yield chunk

    return Response(200, content=StreamedContent(b"image/jpeg", provider))


@app.route("/stream_test2")
async def stream_test2():

    test_url = "https://pbs.twimg.com/media/FkqZQYrakAA5P5h.jpg:orig"

    match = image_name_url_regex.search(test_url)

    if not match:
        print(test_url, "failed")
        return Response(200, content=Content(b"text/plain", b"Not Found"))

    async with client.stream("GET", test_url) as response:
        async def provider():
            async for chunk in response.aiter_bytes(chunk_size=65534):
                yield chunk

        return Response(200, content=StreamedContent(b"image/jpeg", provider))


if __name__ == "__main__":
    if __name__ == "__main__":
        uvicorn.run(app, port=5000, log_level="info")
