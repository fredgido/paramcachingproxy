from typing import TypedDict, Optional

import orjson
from dateutil.parser import parse

from asgi import twitter_url_to_orig

data = orjson.loads(open("temp/2853971.json", "rb").read())

print(data)


tweets = dict[int:dict]()

APIOnedotOneHomeEntry = TypedDict(
    "APIOnedotOneHomeEntry",
    {
        "created_at": str,  # "Tue Dec 27 18:56:23 +0000 2022"
        "id": int,
        "id_str": str,
        "full_text": str,
        "truncated": bool,
        "display_text_range": list[int],
        "entities":dict,
        #     {
        #     "hashtags": [],
        #     "symbols": [],
        #     "user_mentions": [
        #         {
        #             "screen_name": "xx682567",
        #             "name": "682567🔞FANBOX & PATREON",
        #             "id": 3119110320,
        #             "id_str": "3119110320",
        #             "indices": [3, 12],
        #         }
        #     ],
        #     "urls": [],
        #     "media": [
        #         {
        #             "id": 1607389485482016769,
        #             "id_str": "1607389485482016769",
        #             "indices": [39, 62],
        #             "media_url": "http://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
        #             "media_url_https": "https://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
        #             "url": "https://t.co/HM1zpXfLXi",
        #             "display_url": "pic.twitter.com/HM1zpXfLXi",
        #             "expanded_url": "https://twitter.com/xx682567/status/1607389576313856003/photo/1",
        #             "type": "photo",
        #             "original_info": {
        #                 "width": 1838,
        #                 "height": 1500,
        #                 "focus_rects": [
        #                     {"x": 0, "y": 0, "h": 1029, "w": 1838},
        #                     {"x": 214, "y": 0, "h": 1500, "w": 1500},
        #                     {"x": 306, "y": 0, "h": 1500, "w": 1316},
        #                     {"x": 589, "y": 0, "h": 1500, "w": 750},
        #                     {"x": 0, "y": 0, "h": 1500, "w": 1838},
        #                 ],
        #             },
        #             "sizes": {
        #                 "thumb": {"w": 150, "h": 150, "resize": "crop"},
        #                 "small": {"w": 680, "h": 555, "resize": "fit"},
        #                 "large": {"w": 1838, "h": 1500, "resize": "fit"},
        #                 "medium": {"w": 1200, "h": 979, "resize": "fit"},
        #             },
        #             "source_status_id": 1607389576313856003,
        #             "source_status_id_str": "1607389576313856003",
        #             "source_user_id": 3119110320,
        #             "source_user_id_str": "3119110320",
        #             "features": {
        #                 "small": {"faces": []},
        #                 "orig": {"faces": []},
        #                 "large": {"faces": []},
        #                 "medium": {"faces": []},
        #             },
        #         }
        #     ],
        # },
        "extended_entities": dict,
# {
#             "media": [
#                 {
#                     "id": 1607389485482016769,
#                     "id_str": "1607389485482016769",
#                     "indices": [39, 62],
#                     "media_url": "http://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
#                     "media_url_https": "https://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
#                     "url": "https://t.co/HM1zpXfLXi",
#                     "display_url": "pic.twitter.com/HM1zpXfLXi",
#                     "expanded_url": "https://twitter.com/xx682567/status/1607389576313856003/photo/1",
#                     "type": "photo",
#                     "original_info": {
#                         "width": 1838,
#                         "height": 1500,
#                         "focus_rects": [
#                             {"x": 0, "y": 0, "h": 1029, "w": 1838},
#                             {"x": 214, "y": 0, "h": 1500, "w": 1500},
#                             {"x": 306, "y": 0, "h": 1500, "w": 1316},
#                             {"x": 589, "y": 0, "h": 1500, "w": 750},
#                             {"x": 0, "y": 0, "h": 1500, "w": 1838},
#                         ],
#                     },
#                     "sizes": {
#                         "thumb": {"w": 150, "h": 150, "resize": "crop"},
#                         "small": {"w": 680, "h": 555, "resize": "fit"},
#                         "large": {"w": 1838, "h": 1500, "resize": "fit"},
#                         "medium": {"w": 1200, "h": 979, "resize": "fit"},
#                     },
#                     "source_status_id": 1607389576313856003,
#                     "source_status_id_str": "1607389576313856003",
#                     "source_user_id": 3119110320,
#                     "source_user_id_str": "3119110320",
#                     "features": {
#                         "small": {"faces": []},
#                         "orig": {"faces": []},
#                         "large": {"faces": []},
#                         "medium": {"faces": []},
#                     },
#                     "media_key": "3_1607389485482016769",
#                     "ext_alt_text": None,
#                 }
#             ]
#         },
        "source": str, # '<a href="https://mobile.twitter.com" rel="nofollow">Twitter Web App</a>'
        "in_reply_to_status_id": None,
        "in_reply_to_status_id_str": None,
        "in_reply_to_user_id": None,
        "in_reply_to_user_id_str": None,
        "in_reply_to_screen_name": None,
        "user": dict,
        #     {
        #     "id": 1493530938,
        #     "id_str": "1493530938",
        #     "name": "C-Low🍡 お絵描きカラス🌸🎀",
        #     "screen_name": "C_Low_t",
        #     "location": "https://pawoo.net/@C_Low",
        #     "description": "お絵描きカラスです Skeb：https://t.co/N27UBppBtE 一枚絵Skeb：@NoSABUN_CLow Fantia：https://t.co/ydx9RcCIiX",
        #     "url": "https://t.co/Gd6vN5oEx6",
        #     "entities": {
        #         "url": {
        #             "urls": [
        #                 {
        #                     "url": "https://t.co/Gd6vN5oEx6",
        #                     "expanded_url": "http://www.pixiv.net/member.php?id=2457115",
        #                     "display_url": "pixiv.net/member.php?id=…",
        #                     "indices": [0, 23],
        #                 }
        #             ]
        #         },
        #         "description": {
        #             "urls": [
        #                 {
        #                     "url": "https://t.co/N27UBppBtE",
        #                     "expanded_url": "http://skeb.jp/@C_Low_t",
        #                     "display_url": "skeb.jp/@C_Low_t",
        #                     "indices": [15, 38],
        #                 },
        #                 {
        #                     "url": "https://t.co/ydx9RcCIiX",
        #                     "expanded_url": "http://fantia.jp/fanclubs/1736",
        #                     "display_url": "fantia.jp/fanclubs/1736",
        #                     "indices": [68, 91],
        #                 },
        #             ]
        #         },
        #     },
        #     "protected": False,
        #     "followers_count": 150963,
        #     "fast_followers_count": 0,
        #     "normal_followers_count": 150963,
        #     "friends_count": 4921,
        #     "listed_count": 1203,
        #     "created_at": "Sat Jun 08 18:08:38 +0000 2013",
        #     "favourites_count": 73437,
        #     "utc_offset": None,
        #     "time_zone": None,
        #     "geo_enabled": False,
        #     "verified": False,
        #     "statuses_count": 62755,
        #     "media_count": 3819,
        #     "lang": None,
        #     "contributors_enabled": False,
        #     "is_translator": False,
        #     "is_translation_enabled": False,
        #     "profile_background_color": "C0DEED",
        #     "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
        #     "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
        #     "profile_background_tile": False,
        #     "profile_image_url": "http://pbs.twimg.com/profile_images/742246335147958272/TRhbC3vB_normal.jpg",
        #     "profile_image_url_https": "https://pbs.twimg.com/profile_images/742246335147958272/TRhbC3vB_normal.jpg",
        #     "profile_banner_url": "https://pbs.twimg.com/profile_banners/1493530938/1465800219",
        #     "profile_image_extensions_alt_text": None,
        #     "profile_banner_extensions_alt_text": None,
        #     "profile_link_color": "1DA1F2",
        #     "profile_sidebar_border_color": "C0DEED",
        #     "profile_sidebar_fill_color": "DDEEF6",
        #     "profile_text_color": "333333",
        #     "profile_use_background_image": True,
        #     "has_extended_profile": True,
        #     "default_profile": True,
        #     "default_profile_image": False,
        #     "has_custom_timelines": False,
        #     "can_media_tag": True,
        #     "followed_by": False,
        #     "following": True,
        #     "follow_request_sent": False,
        #     "notifications": False,
        #     "business_profile_state": "none",
        #     "translator_type": "none",
        #     "withheld_in_countries": [],
        #     "require_some_consent": False,
        # },
        "geo": None,
        "coordinates": None,
        "place": None,
        "contributors": None,
        "retweeted_status": Optional["APIOnedotOneHomeEntry"],
        #     {
        #     "created_at": "Mon Dec 26 14:55:13 +0000 2022",
        #     "id": 1607389576313856003,
        #     "id_str": "1607389576313856003",
        #     "full_text": "遅れたけどみんなメリクリᐠ( ᐛ )ᐟ！🎄✨🍑💕 https://t.co/HM1zpXfLXi",
        #     "truncated": False,
        #     "display_text_range": [0, 24],
        #     "entities": {
        #         "hashtags": [],
        #         "symbols": [],
        #         "user_mentions": [],
        #         "urls": [],
        #         "media": [
        #             {
        #                 "id": 1607389485482016769,
        #                 "id_str": "1607389485482016769",
        #                 "indices": [25, 48],
        #                 "media_url": "http://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
        #                 "media_url_https": "https://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
        #                 "url": "https://t.co/HM1zpXfLXi",
        #                 "display_url": "pic.twitter.com/HM1zpXfLXi",
        #                 "expanded_url": "https://twitter.com/xx682567/status/1607389576313856003/photo/1",
        #                 "type": "photo",
        #                 "original_info": {
        #                     "width": 1838,
        #                     "height": 1500,
        #                     "focus_rects": [
        #                         {"x": 0, "y": 0, "h": 1029, "w": 1838},
        #                         {"x": 214, "y": 0, "h": 1500, "w": 1500},
        #                         {"x": 306, "y": 0, "h": 1500, "w": 1316},
        #                         {"x": 589, "y": 0, "h": 1500, "w": 750},
        #                         {"x": 0, "y": 0, "h": 1500, "w": 1838},
        #                     ],
        #                 },
        #                 "sizes": {
        #                     "thumb": {"w": 150, "h": 150, "resize": "crop"},
        #                     "small": {"w": 680, "h": 555, "resize": "fit"},
        #                     "large": {"w": 1838, "h": 1500, "resize": "fit"},
        #                     "medium": {"w": 1200, "h": 979, "resize": "fit"},
        #                 },
        #                 "features": {
        #                     "small": {"faces": []},
        #                     "orig": {"faces": []},
        #                     "large": {"faces": []},
        #                     "medium": {"faces": []},
        #                 },
        #             }
        #         ],
        #     },
        #     "extended_entities": {
        #         "media": [
        #             {
        #                 "id": 1607389485482016769,
        #                 "id_str": "1607389485482016769",
        #                 "indices": [25, 48],
        #                 "media_url": "http://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
        #                 "media_url_https": "https://pbs.twimg.com/media/Fk6YOIHagAENaJT.jpg",
        #                 "url": "https://t.co/HM1zpXfLXi",
        #                 "display_url": "pic.twitter.com/HM1zpXfLXi",
        #                 "expanded_url": "https://twitter.com/xx682567/status/1607389576313856003/photo/1",
        #                 "type": "photo",
        #                 "original_info": {
        #                     "width": 1838,
        #                     "height": 1500,
        #                     "focus_rects": [
        #                         {"x": 0, "y": 0, "h": 1029, "w": 1838},
        #                         {"x": 214, "y": 0, "h": 1500, "w": 1500},
        #                         {"x": 306, "y": 0, "h": 1500, "w": 1316},
        #                         {"x": 589, "y": 0, "h": 1500, "w": 750},
        #                         {"x": 0, "y": 0, "h": 1500, "w": 1838},
        #                     ],
        #                 },
        #                 "sizes": {
        #                     "thumb": {"w": 150, "h": 150, "resize": "crop"},
        #                     "small": {"w": 680, "h": 555, "resize": "fit"},
        #                     "large": {"w": 1838, "h": 1500, "resize": "fit"},
        #                     "medium": {"w": 1200, "h": 979, "resize": "fit"},
        #                 },
        #                 "features": {
        #                     "small": {"faces": []},
        #                     "orig": {"faces": []},
        #                     "large": {"faces": []},
        #                     "medium": {"faces": []},
        #                 },
        #                 "media_key": "3_1607389485482016769",
        #                 "ext_alt_text": None,
        #             }
        #         ]
        #     },
        #     "source": '<a href="https://mobile.twitter.com" rel="nofollow">Twitter Web App</a>',
        #     "in_reply_to_status_id": None,
        #     "in_reply_to_status_id_str": None,
        #     "in_reply_to_user_id": None,
        #     "in_reply_to_user_id_str": None,
        #     "in_reply_to_screen_name": None,
        #     "user": {
        #         "id": 3119110320,
        #         "id_str": "3119110320",
        #         "name": "682567🔞FANBOX & PATREON",
        #         "screen_name": "xx682567",
        #         "location": "",
        #         "description": "🔞18歳未満禁止です!\n💕PIXIV : https://t.co/wIL3gyUfTb\n💕FANBOX : https://t.co/hByDHQCrTn\n💕PATREON : https://t.co/JV2jwJTUoi\n💕GUMROAD : https://t.co/GtSzWCFXGQ",
        #         "url": "https://t.co/Lew9oT3Ck1",
        #         "entities": {
        #             "url": {
        #                 "urls": [
        #                     {
        #                         "url": "https://t.co/Lew9oT3Ck1",
        #                         "expanded_url": "https://picarto.tv/682567",
        #                         "display_url": "picarto.tv/682567",
        #                         "indices": [0, 23],
        #                     }
        #                 ]
        #             },
        #             "description": {
        #                 "urls": [
        #                     {
        #                         "url": "https://t.co/wIL3gyUfTb",
        #                         "expanded_url": "http://pixiv.net/users/682567",
        #                         "display_url": "pixiv.net/users/682567",
        #                         "indices": [21, 44],
        #                     },
        #                     {
        #                         "url": "https://t.co/hByDHQCrTn",
        #                         "expanded_url": "http://xx682567.fanbox.cc",
        #                         "display_url": "xx682567.fanbox.cc",
        #                         "indices": [55, 78],
        #                     },
        #                     {
        #                         "url": "https://t.co/JV2jwJTUoi",
        #                         "expanded_url": "http://patreon.com/xx682567",
        #                         "display_url": "patreon.com/xx682567",
        #                         "indices": [90, 113],
        #                     },
        #                     {
        #                         "url": "https://t.co/GtSzWCFXGQ",
        #                         "expanded_url": "http://xx682567.gumroad.com",
        #                         "display_url": "xx682567.gumroad.com",
        #                         "indices": [125, 148],
        #                     },
        #                 ]
        #             },
        #         },
        #         "protected": False,
        #         "followers_count": 89228,
        #         "fast_followers_count": 0,
        #         "normal_followers_count": 89228,
        #         "friends_count": 943,
        #         "listed_count": 575,
        #         "created_at": "Tue Mar 31 05:10:30 +0000 2015",
        #         "favourites_count": 41243,
        #         "utc_offset": None,
        #         "time_zone": None,
        #         "geo_enabled": False,
        #         "verified": False,
        #         "statuses_count": 43103,
        #         "media_count": 62,
        #         "lang": None,
        #         "contributors_enabled": False,
        #         "is_translator": False,
        #         "is_translation_enabled": False,
        #         "profile_background_color": "000000",
        #         "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
        #         "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
        #         "profile_background_tile": False,
        #         "profile_image_url": "http://pbs.twimg.com/profile_images/1602327533042929664/pPmPRUV0_normal.jpg",
        #         "profile_image_url_https": "https://pbs.twimg.com/profile_images/1602327533042929664/pPmPRUV0_normal.jpg",
        #         "profile_banner_url": "https://pbs.twimg.com/profile_banners/3119110320/1645358352",
        #         "profile_image_extensions_alt_text": None,
        #         "profile_banner_extensions_alt_text": None,
        #         "profile_link_color": "E81C4F",
        #         "profile_sidebar_border_color": "000000",
        #         "profile_sidebar_fill_color": "000000",
        #         "profile_text_color": "000000",
        #         "profile_use_background_image": False,
        #         "has_extended_profile": True,
        #         "default_profile": False,
        #         "default_profile_image": False,
        #         "has_custom_timelines": True,
        #         "can_media_tag": True,
        #         "followed_by": False,
        #         "following": False,
        #         "follow_request_sent": False,
        #         "notifications": False,
        #         "business_profile_state": "none",
        #         "translator_type": "none",
        #         "withheld_in_countries": [],
        #         "require_some_consent": False,
        #     },
        #     "geo": None,
        #     "coordinates": None,
        #     "place": None,
        #     "contributors": None,
        #     "is_quote_status": False,
        #     "retweet_count": 712,
        #     "favorite_count": 5617,
        #     "reply_count": 26,
        #     "conversation_id": 1607389576313856003,
        #     "conversation_id_str": "1607389576313856003",
        #     "favorited": False,
        #     "retweeted": False,
        #     "possibly_sensitive": True,
        #     "possibly_sensitive_appealable": False,
        #     "lang": "ja",
        #     "supplemental_language": None,
        # },
        "is_quote_status": bool,
        "retweet_count": int,
        "favorite_count": int,
        "reply_count": int,
        "conversation_id": int,
        "conversation_id_str": str,
        "favorited": bool,
        "retweeted": bool,
        "possibly_sensitive": bool,
        "possibly_sensitive_appealable": bool,
        "lang": Optional[str],
        "supplemental_language":Optional[str],
    },
)

for entry in data:
    entry : APIOnedotOneHomeEntry
    tweet_id = entry["id_str"]
    full_text = entry["full_text"]
    language = entry["lang"]
    retweet_count = entry["retweet_count"]
    favorite_count = entry["favorite_count"]
    reply_count = entry["reply_count"]
    is_quote_status = entry["is_quote_status"]
    views = None
    conversation_id = entry["conversation_id_str"]
    hashtags = entry["entities"]["hashtags"]
    symbols = entry["entities"]["symbols"]
    user_mentions = entry["entities"]["user_mentions"]
    urls = entry["entities"]["urls"]
    media = list[dict]()
    is_retweet = bool(entry.get("retweeted_status"))

    for asset in entry["extended_entities"]:
        subdomain, url_type, name, extension = twitter_url_to_orig(asset["media_url_https"])
        if not name:
            raise Exception("no name")
        media_asset = {
            "id":asset["id_str"],
            "url": asset["media_url_https"],
            "width" : asset["original_info"]["width"],
            "height": asset["original_info"]["height"],
            "post_id" : asset["source_status_id_str"],
            "name": name,
            "extension":extension,
            "ext_alt_text" : asset["ext_alt_text"]
            #datetime nullable from file request
        }
        media.append(media_asset)
    users = list[dict]()
    users.append(
    {
        "id": entry["user"]["id_str"],
        "created_at": parse(entry["user"]["created_at"]),
        "name": entry["user"]["name"],
        "screen_name": entry["user"]["screen_name"],
        "location":entry["user"]["location"],
        "description": entry["user"]["description"],
        "location": entry["user"]["location"],
        "urls": (
            [u["expanded_url"] for u in entry["user"]["entities"]["url"]["urls"]
            +
             [u["expanded_url"] for u in entry["user"]["entities"]["description"]["urls"]
        )
             ,
        "protected": entry["user"]["protected"],
        "followers_count": entry["user"]["followers_count"],
        "friends_count": entry["user"]["friends_count"],
        "listed_count": entry["user"]["listed_count"],
        "statuses_count": entry["user"]["statuses_count"],
        "media_count": entry["user"]["media_count"],
        "profile_image_url_https": entry["user"]["profile_image_url_https"],
        "profile_banner_url": entry["user"]["profile_banner_url"],

        # datetime nullable from file request
    }
    )
