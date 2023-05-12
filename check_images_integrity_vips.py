import itertools
import pathlib

import pyvips

base_dir = "/mnt/p300_3tb/twitter_media/all/twitter_media"
dest_dir = "/mnt/p300_3tb/twitter_media/all/trash2"
bad_images = []


for file in pathlib.Path(f"{base_dir}").glob("**/**/*"):
    if (not file.is_file() or ".mp4" in file.name) or not (
        ".jpg" in file.name or ".jpeg" in file.name or ".png" in file.name or ".gif" in file.name
    ):
        continue
    try:
        img = pyvips.Image.new_from_file(str(file), access="sequential", fail=True)  # pyvips.enums.FailOn.WARNING)
        target = pyvips.vtarget.Target.new_to_memory()
        buffer = img.ppmsave_target(target)
        # print(len(target.get("blob")))
    except Exception as e:
        print(str(file))
        if file.stat().st_size < 700:
            print("empty_file", file)
        elif "truncated" in e.args[0]:
            print("truncated_file", file)
        else:
            print("cannot decode", file, e)

        move_path = pathlib.Path(dest_dir) / file.parent.name / file.name
        move_path.parent.mkdir(exist_ok=True,parents=True)
        print("moved")
        print(file)
        print(move_path)
        file.rename(move_path)
        continue
