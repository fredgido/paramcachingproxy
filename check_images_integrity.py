import pathlib

import PIL
import pyvips
from PIL import Image
from PIL.Image import Transpose

MEDIA_FOLDER = "/media/fredgido/p300_3tb/twitter_media/all"
bad_images = []

for file in pathlib.Path(f"{MEDIA_FOLDER}/twitter_media").glob("*"):
    if not file.is_file() or ".mp4" in file.name:
        continue
    try:
        #img = pyvips.Image.new_from_file(str(file), access='sequential')
        im = Image.open(str(file))
        #im.verify()  # I perform also verify, don't know if he sees other types o defects
        im.transpose(Transpose.FLIP_LEFT_RIGHT)
        im.close()
    except Exception as e:
        if file.stat().st_size < 700:
            print("empty_file", file)
        elif "truncated" in e.args[0]:
            print("truncated_file",file)
        else:
            print("cannot decode", file, e)
        continue
