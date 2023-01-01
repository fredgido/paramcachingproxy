import json
from pathlib import Path

all_text = ""

for file in Path("salvage").glob("*"):
    if not file.is_file():
        continue
    all_text += file.open(mode="rb").read().decode("utf-8","replace")


import re

img_regex = re.compile(r"""https:\/\/pbs\.twimg\.com\/media\/[^"\\]*""")

tweet_regex = re.compile(r"""https:\/\/twitter\.com\/[^"]*\/status\/[\d]*""")

# open("salvage_img.json","w").write(json.dumps(sorted(set(img_regex.findall(all_text)))))
# open("salvage_tweet.json","w").write(json.dumps(sorted(set(tweet_regex.findall(all_text)))))
open("salvage_img.json","w").write("\n".join(sorted(set(img_regex.findall(all_text)))))
open("salvage_tweet.json","w").write("\n".join(sorted(set(tweet_regex.findall(all_text)))))
