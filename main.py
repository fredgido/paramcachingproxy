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

image_name_url =1

@app.route("/stream_test")
async def get_movies():
    from blacksheep.client import ClientSession

    test_url = "https://pbs.twimg.com/media/FkqZQYrakAA5P5h.jpg:orig"

    async def client_example(loop):
        async with ClientSession() as client:
            response = await client.get(test_url)

            assert response is not None
            text = await response.text()
            print(text)

    async def provider():
        async with client.stream("GET", test_url) as response:
            async for chunk in response.aiter_bytes():
                yield chunk

    return Response(200, content=StreamedContent(b"image/jpeg", provider))


if __name__ == "__main__":
    if __name__ == "__main__":
        uvicorn.run(app, port=5000, log_level="info")
