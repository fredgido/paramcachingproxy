import asyncio
import re

import asyncpg
import orjson
from asyncpg import Connection
from dateutil.parser import parse

from asgi import twitter_url_to_orig
from db import user_insert_statement, user_vars, asset_vars, asset_insert_statement, post_insert_statement, post_vars
from pg_connection_creds import connection_creds
from process_entries_typing import APIOnedotOneHomeEntry


def extract_tweet_data(entry):
    tweet = dict(
        id=int(entry["id_str"]),
        full_text=entry["full_text"],
        language=entry["lang"],
        retweet_count=entry["retweet_count"],
        favorite_count=entry["favorite_count"],
        reply_count=entry["reply_count"],
        is_quote_status=entry["is_quote_status"],
        views=None,
        conversation_id=int(entry["conversation_id_str"]) if entry["conversation_id_str"] else None,
        hashtags=[hashtag["text"] for hashtag in entry["entities"]["hashtags"]],
        symbols=entry["entities"]["symbols"],
        user_mentions=[int(mention["id_str"]) for mention in entry["entities"]["user_mentions"]],
        urls=[url["expanded_url"] for url in entry["entities"]["urls"]],
        is_retweet=bool(entry.get("retweeted_status")),
    )
    return tweet


twitter_video_url_regex_str = r"(?P<subdomain>[^.\/]*)\.twimg\.com\/(?P<type>ext_tw_video|amplify_video)/(?P<id>[\d]*)/(?P<pu>pu/)?vid/(?P<resolution>[x\d]*)/(?P<name>[a-zA-Z\d_-]*)(?:\?format=|\.)(?P<extension>[a-zA-Z0-9]{3,4})"
twitter_url_regex = re.compile(twitter_video_url_regex_str)


def twitter_video_url_to_orig(twitter_url: str):
    match = twitter_url_regex.search(twitter_url)
    if not match:
        return None, None, None, None, None

    capture_groups = match.groupdict()
    return (
        capture_groups["subdomain"],
        capture_groups["type"],
        capture_groups["name"],
        capture_groups["extension"],
        capture_groups["resolution"],
    )


def extract_assets_data(entry, tweet_id: int) -> list[dict]:
    media = list[dict]()
    if "extended_entities" not in entry:
        return media
    for asset in entry["extended_entities"]["media"]:
        if asset["type"] == "image":
            subdomain, url_type, name, extension = twitter_url_to_orig(asset["media_url_https"])
            if not name:
                raise Exception("no name")
            media_asset = {
                "id": int(asset["id_str"]),
                "url": asset["media_url_https"],
                "width": asset["original_info"]["width"],
                "height": asset["original_info"]["height"],
                "post_id": int(asset.get("source_status_id_str")) or tweet_id,
                "name": name,
                "extension": extension,
                "ext_alt_text": asset["ext_alt_text"]
                # datetime nullable from file request
            }
            media.append(media_asset)
        elif asset["type"] == "video":
            variants = sorted(asset["video_info"]["variants"], key=lambda x: x.get("bitrate", 0), reverse=True)
            video_url = variants[0]["url"].split("?")[0]
            subdomain, url_type, name, extension, resolution = twitter_video_url_to_orig(video_url)

            if not name:
                raise Exception("no name")
            if not asset.get("source_status_id_str"):
                post_id_from_url = int(asset["expanded_url"].split("/")[5])
                if post_id_from_url != tweet_id:
                    raise Exception("Different id")
            else:
                tweet_id = int(asset["source_status_id_str"])
            media_asset = {
                "id": int(asset["id_str"]),
                "url": video_url,
                "width": asset["original_info"]["width"],
                "height": asset["original_info"]["height"],
                "post_id": tweet_id,
                "name": name,
                "extension": extension,
                "ext_alt_text": asset["ext_alt_text"]
                # datetime nullable from file request
            }
            media.append(media_asset)
    return media


def extract_users_data(entry) -> list[dict]:
    users = list[dict]()
    users.append(
        {
            "id": int(entry["user"]["id_str"]),
            "created_at": parse(entry["user"]["created_at"]),
            "name": entry["user"]["name"],
            "screen_name": entry["user"]["screen_name"],
            "location": entry["user"]["location"],
            "description": entry["user"]["description"],
            "urls": (
                [u["expanded_url"] for u in entry["user"]["entities"].get("url", {}).get("urls", [])]
                + [u["expanded_url"] for u in entry["user"]["entities"]["description"]["urls"]]
            ),
            "protected": entry["user"]["protected"],
            "followers_count": entry["user"]["followers_count"],
            "friends_count": entry["user"]["friends_count"],
            "listed_count": entry["user"]["listed_count"],
            "statuses_count": entry["user"]["statuses_count"],
            "media_count": entry["user"]["media_count"],
            "profile_image_url_https": entry["user"]["profile_image_url_https"],
            "profile_banner_url": entry["user"].get("profile_banner_url"),
            "profile_background_image_url_https": entry["user"].get("profile_background_image_url_https"),
            # datetime nullable from file request
        }
    )
    return users


def process_data(api_data):
    tweets_parsed = dict[int:dict]()
    assets_parsed = dict[tuple[str, str] : dict]()
    users_parsed = dict[int:dict]()

    for tweet_entry in api_data:
        tweet_entry: APIOnedotOneHomeEntry
        tweet = extract_tweet_data(tweet_entry)
        tweets_parsed[int(tweet["id"])] = tweet
        assets_data = extract_assets_data(tweet_entry, int(tweet["id"]))
        for asset_data in assets_data:
            assets_parsed[(asset_data["name"], asset_data["extension"])] = asset_data
        users_data = extract_users_data(tweet_entry)
        for user_data in users_data:
            users_parsed[int(user_data["id"])] = user_data

        if tweet_entry.get("retweeted_status"):
            retweet = extract_tweet_data(tweet_entry["retweeted_status"])
            tweets_parsed[int(retweet["id"])] = retweet
            retweet_assets_data = extract_assets_data(tweet_entry["retweeted_status"], int(retweet["id"]))
            for retweet_asset_data in retweet_assets_data:
                assets_parsed[(retweet_asset_data["name"], retweet_asset_data["extension"])] = retweet_asset_data
            users_data = extract_users_data(tweet_entry["retweeted_status"])
            for user_data in users_data:
                users_parsed[int(user_data["id"])] = user_data
    return tweets_parsed, assets_parsed, users_parsed


async def run():
    db_con: Connection = await asyncpg.connect(**connection_creds)
    get_statement = """
    SELECT id, url, "data", created_at, processed_at
    FROM public.api_dump
    where url like 'https://api.twitter.com/1.1/statuses/home_timeline.json%' and processed_at is null
    order by id asc limit 10000"""

    insert_tweets = dict()
    insert_assets = dict()
    insert_users = dict()
    row_ids = list()

    for row in await db_con.fetch(get_statement):
        api_dump_id, url, data, created_at, processed_at = row
        row_ids.append(api_dump_id)
        tweets, assets, users = process_data(orjson.loads(data))

        for user in users.values():
            user["processed_at"] = created_at
        for asset in assets.values():
            asset["processed_at"] = created_at
            asset["file_header_date"] = None
        for tweet in tweets.values():
            tweet["processed_at"] = created_at

        insert_tweets.update(tweets)
        insert_assets.update(assets)
        insert_users.update(users)

    values = await db_con.executemany(
        post_insert_statement, [[tweet[var] for var in post_vars] for tweet in insert_tweets.values()]
    )
    print(values)
    values = await db_con.executemany(
        asset_insert_statement, [[asset[var] for var in asset_vars] for asset in insert_assets.values()]
    )
    print(values)
    values = await db_con.executemany(
        user_insert_statement, [[user[var] for var in user_vars] for user in insert_users.values()]
    )
    print(values)

    await db_con.close()


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    loop.run_until_complete(run())
