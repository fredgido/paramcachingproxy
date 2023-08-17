import os
import pathlib

from collections import defaultdict

import httpx
from httpx import ReadTimeout

from asgi import twitter_url_to_orig

MEDIA_FOLDER = "/mnt/p300_3tb/twitter_media/all"
trash_folder = "/mnt/p300_3tb/twitter_media/all/trash5"
retrash = f"{MEDIA_FOLDER}/trash5_retrash"

bad_images = []

process_trash = False
move_trash= True

if not process_trash:
    for file in pathlib.Path(f"{MEDIA_FOLDER}/twitter_media").glob("*"):
        if not file.is_file():
            continue
        file_stat = file.stat()
        if file_stat.st_size < 200:
            print(file.name)
            bad_images.append(file.name)
else:
    pathlib.Path(retrash).mkdir(exist_ok=True,parents=True)
    for file in pathlib.Path(f"{trash_folder}/twitter_media").glob("**/**/*"):
        if not file.is_file():
            continue
        bad_images.append(file.name)


print(bad_images)

base_image_name__bad_images = defaultdict(set)

UUID_LEN = len("24295a93-5f14-4217-bb51-71cbe2e8413d")
for image in bad_images:
    if len(image) > UUID_LEN:
        base_image_name__bad_images[image[: len(image) - UUID_LEN]].add(image)
    else:
        base_image_name__bad_images[image].add(image)


for image, files in base_image_name__bad_images.items():
    if image not in files and pathlib.Path(f"{MEDIA_FOLDER}/twitter_media/{image}").exists(): # good image exists
        print("exists", image)
        import shutil


        if pathlib.Path(f"{MEDIA_FOLDER}/twitter_media/{image}").exists():
            shutil.copy(f"{MEDIA_FOLDER}/twitter_media/{image}", f"{retrash}/{image}")
        elif pathlib.Path(f"{trash_folder}/twitter_media/{image}").exists():
            os.rename(f"{trash_folder}/twitter_media/{image}", f"{retrash}/{image}")
        for each in files:
            if pathlib.Path(f"{MEDIA_FOLDER}/twitter_media/{each}").exists():
                os.rename(f"{MEDIA_FOLDER}/twitter_media/{each}", f"{retrash}/{each}")
            elif pathlib.Path(f"{trash_folder}/twitter_media/{each}").exists():
                os.rename(f"{trash_folder}/twitter_media/{each}", f"{retrash}/{each}")
        continue

    if "mp4" in image:
        download_url = f"https://video.twimg.com/tweet_video/{image}"
    else:
        download_url = f"https://pbs.twimg.com/media/{image}:orig"


    try:
        r = httpx.get(download_url,timeout=30)
    except ReadTimeout:
        print("tiemout", download_url)
        raise
    if r.status_code == 200:
        subdomain, url_type, name, extension = twitter_url_to_orig(download_url)
        print("  valid", download_url, f"{name}.{extension}")

        for each in files:
            if pathlib.Path(f"{MEDIA_FOLDER}/twitter_media/{each}").exists():
                os.rename(f"{MEDIA_FOLDER}/twitter_media/{each}", f"{retrash}/{each}")
            if pathlib.Path(f"{trash_folder}/twitter_media/{each}").exists():
                os.rename(f"{trash_folder}/twitter_media/{each}", f"{retrash}/{each}")

        open(f"{MEDIA_FOLDER}/twitter_media/{name}.{extension}", "wb").write(r.content)

    else:
        print("invalid", r.status_code, download_url)
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
                if pathlib.Path(f"{MEDIA_FOLDER}/twitter_media/{each}").exists():
                    os.rename(f"{MEDIA_FOLDER}/twitter_media/{each}", f"{retrash}/{each}")
                elif pathlib.Path(f"{trash_folder}/twitter_media/{each}").exists():
                    os.rename(f"{trash_folder}/twitter_media/{each}", f"{retrash}/{each}")
        else:
            print("invalid second try", r.status_code, download_url)
            if move_trash:
                if pathlib.Path(f"{trash_folder}/twitter_media/{image}").exists():
                    os.rename(f"{trash_folder}/twitter_media/{image}", f"{retrash}/{image}")
                else:
                    for each_glob in pathlib.Path(f"{trash_folder}/twitter_media").glob(f"{image}*"):
                        os.rename(each_glob, f"{retrash}/{each_glob.name}")