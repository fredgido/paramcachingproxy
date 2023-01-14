import asyncio

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
        hashtags=entry["entities"]["hashtags"],
        symbols=entry["entities"]["symbols"],
        user_mentions=[int(mention["id_str"]) for mention in entry["entities"]["user_mentions"]],
        urls=entry["entities"]["urls"],
        is_retweet=bool(entry.get("retweeted_status")),
    )
    return tweet


def extract_assets_data(entry, tweet_id=None) -> list[dict]:
    media = list[dict]()
    for asset in entry["extended_entities"]["media"]:
        subdomain, url_type, name, extension = twitter_url_to_orig(asset["media_url_https"])
        if not name:
            raise Exception("no name")
        media_asset = {
            "id": int(asset["id_str"]),
            "url": asset["media_url_https"],
            "width": asset["original_info"]["width"],
            "height": asset["original_info"]["height"],
            "post_id": asset.get("source_status_id_str") or tweet_id,
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
                    [u["expanded_url"] for u in entry["user"]["entities"]["url"]["urls"]]
                    + [u["expanded_url"] for u in entry["user"]["entities"]["description"]["urls"]]
            ),
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
    return users


def process_data(api_data):
    tweets_parsed = dict[int:dict]()
    assets_parsed = dict[tuple[str, str]: dict]()
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

        if tweet_entry["retweeted_status"]:
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

    # statement = """INSERT INTO public.api_dump (url, "data",created_at) VALUES($1, $2, $3);"""
    # values = await db_con.executemany(statement, [("localhost", b"B", datetime.datetime.utcnow())])
    # print(values)

    get_statement = """SELECT id, url, "data", created_at, processed_at FROM public.api_dump where id = 2853971 order by id asc limit 100000"""

    api_dump_id, url, data, created_at, processed_at = (await db_con.fetch(get_statement))[0]
    tweets, assets, users = process_data(orjson.loads(data))

    for user in users.values():
        user["processed_at"] = created_at

    values = await db_con.executemany(
        user_insert_statement, [[user[var] for var in user_vars] for user in users.values()]
    )
    print(values)

    for asset in assets.values():
        asset["processed_at"] = created_at
        asset["file_header_date"] = None

    values = await db_con.executemany(
        asset_insert_statement, [[asset[var] for var in asset_vars] for asset in assets.values()]
    )
    print(values)

    for tweet in tweets.values():
        tweet["processed_at"] = created_at
        tweet["file_header_date"] = None

    values = await db_con.executemany(
        post_insert_statement, [[tweet[var] for var in post_vars] for tweet in tweets.values()]
    )
    print(values)

    await db_con.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
