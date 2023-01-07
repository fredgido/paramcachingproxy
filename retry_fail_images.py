import os
import pathlib

from collections import defaultdict

import httpx

from asgi import twitter_url_to_orig

MEDIA_FOLDER = "/media/fredfernandes/Elements/twitter_dump/twitter_media"
bad_images = []

for file in pathlib.Path(f"{MEDIA_FOLDER}/twitter_media").glob("*"):
    if not file.is_file():
        continue
    file_stat = file.stat()
    if file_stat.st_size < 700:
        bad_images.append(file.name)

print(bad_images)

uuid_removed_bad_images = defaultdict(set)

UUID_LEN = len("24295a93-5f14-4217-bb51-71cbe2e8413d")
for image in bad_images:
    if len(image) > UUID_LEN:
        uuid_removed_bad_images[image[: len(image) - UUID_LEN]].add(image)
    else:
        uuid_removed_bad_images[image].add(image)


for image, files in uuid_removed_bad_images.items():
    if image not in files and pathlib.Path(f"{MEDIA_FOLDER}/twitter_media/{image}").exists():
        print("exists", image)
        continue

    if "mp4" in image:
        download_url = f"https://video.twimg.com/tweet_video/{image}"
    else:
        download_url = f"https://pbs.twimg.com/media/{image}:orig"

    r = httpx.get(download_url)
    if r.status_code == 200:
        subdomain, url_type, name, extension = twitter_url_to_orig(download_url)
        print("  valid", download_url, f"{name}.{extension}")
        open(f"{MEDIA_FOLDER}/twitter_media/{name}.{extension}", "wb").write(r.content)

    else:
        print("invalid", download_url)
        if ".png" in download_url:
            download_url = download_url.replace("png", "jpg")
            extension = "jpg"
        elif ".jpg" in download_url:
            download_url = download_url.replace("jpg", "png")
            extension = "png"
        else:
            continue
        r = httpx.get(download_url)
        if r.status_code == 200:
            subdomain, url_type, name, extension = twitter_url_to_orig(download_url)
            print("R valid", download_url, f"{name}.{extension}")
            open(f"{MEDIA_FOLDER}/twitter_media/{name}.{extension}", "wb").write(r.content)
            for each in files:
                os.rename(f"{MEDIA_FOLDER}/twitter_media/{each}", f"{MEDIA_FOLDER}/trash/{each}")
        else:
            print("invalid", download_url)
