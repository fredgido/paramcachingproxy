import pathlib

import httpx
import uvicorn
from blacksheep import Application, Response, Content, StreamedContent
import re

twitter_media_path = pathlib.Path("twitter_media")
twitter_media_path.mkdir(exist_ok=True)

image_name_url_regex_str = r"\/media\/(?P<name>[a-zA-Z\d]*)\.(?P<extension>[a-z]*)"

image_name_url_regex = re.compile(image_name_url_regex_str)

app = Application()
client = httpx.AsyncClient()


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


if __name__ == "__main__":
    if __name__ == "__main__":
        uvicorn.run(app, port=5000, log_level="info")
