import asyncio
import datetime
import fnmatch
import re

import asyncpg
import orjson
from asyncpg import Connection
from dateutil.parser import parse

from asgi import twitter_url_to_orig
from db import (
    user_insert_statement,
    user_vars,
    asset_vars,
    asset_insert_statement,
    post_insert_statement,
    post_vars,
    api_dump_update_processed,
)
from pg_connection_creds import connection_creds
from process_entries_typing import APIOnedotOneHomeEntry


def extract_tweet_data_legacy(entry):
    tweet = dict(
        id=int(entry["id_str"]),
        user_id=int(entry["user"]["id_str"]) if entry.get("user") else int(entry["user_id_str"]),
        full_text=entry["full_text"],
        language=entry["lang"],
        retweet_count=entry["retweet_count"],
        favorite_count=entry["favorite_count"],
        reply_count=entry["reply_count"],
        is_quote_status=entry["is_quote_status"],
        views=None,
        conversation_id=int(entry["conversation_id_str"]) if entry["conversation_id_str"] else None,
        hashtags=[hashtag["text"] for hashtag in entry["entities"]["hashtags"]],
        symbols=[symbol["text"] for symbol in entry["entities"]["symbols"]],
        user_mentions=[int(mention["id_str"]) for mention in entry["entities"]["user_mentions"]],
        urls=[url["expanded_url"] for url in entry["entities"]["urls"]],
        is_retweet=bool(entry.get("retweeted_status")),
    )
    return tweet


def extract_tweet_data_timeline(top_entry):
    if top_entry.get("__typename") == "TweetWithVisibilityResults" and "tweet" in top_entry:
        top_entry = top_entry["tweet"]

    if top_entry.get("unmention_data"):
        print(top_entry.get("unmention_data"))
    if top_entry.get("__typename") == "TweetUnavailable":
        return {}
    tweet = extract_tweet_data_legacy(top_entry["legacy"])
    tweet["views"] = int(top_entry.get("views", {}).get("count", 0))
    top_entry.update(tweet)
    return top_entry


twitter_video_url_regex_str = r"(?P<subdomain>[^.\/]*)\.twimg\.com\/(?P<type>ext_tw_video|amplify_video)/(?P<id>[\d]*)/(?P<pu>pu/|pr/)?vid/(?P<resolution>[x\d]*)/(?P<name>[a-zA-Z\d_-]*)(?:\?format=|\.)(?P<extension>[a-zA-Z0-9]{3,4})"
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
        if asset["type"] in ("photo", "animated_gif"):
            subdomain, url_type, name, extension = twitter_url_to_orig(asset["media_url_https"])
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
                "url": asset["media_url_https"],
                "width": asset["original_info"]["width"],
                "height": asset["original_info"]["height"],
                "post_id": tweet_id,
                "name": name,
                "extension": extension,
                "ext_alt_text": asset.get("ext_alt_text")
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
                "ext_alt_text": asset.get("ext_alt_text"),
                # datetime nullable from file request
            }
            media.append(media_asset)
        else:
            print(asset["type"])
    return media


def extract_users_data_legacy(entry) -> list[dict]:
    users = list[dict]()
    users.append(
        {
            "id": int(entry["user"]["rest_id"]) if entry["user"].get("rest_id") else int(entry["user"]["id_str"]),
            "created_at": parse(entry["user"]["created_at"]),
            "name": entry["user"]["name"],
            "screen_name": entry["user"]["screen_name"],
            "location": entry["user"]["location"],
            "description": entry["user"]["description"],
            "urls": (
                [u.get("expanded_url", u.get("url")) for u in entry["user"]["entities"].get("url", {}).get("urls", [])]
                + [u.get("expanded_url", u.get("url")) for u in entry["user"]["entities"]["description"]["urls"]]
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


def extract_users_data_timeline(top_entry):
    legacy = top_entry["legacy"]
    legacy.update(top_entry)
    user = extract_users_data_legacy({"user": legacy})
    return user


def process_data_url(api_data, url: str):
    if url.startswith("https://api.twitter.com/1.1/statuses/home_timeline.json"):
        return process_data_legacy(api_data)
    elif fnmatch.fnmatch(url, "https://api.twitter.com/graphql/*/HomeTimeline*"):

        return process_timeline_data(api_data)
    else:
        raise Exception


def process_timeline_data(data):
    tweets_parsed = dict[int:dict]()
    assets_parsed = dict[tuple[str, str] : dict]()
    users_parsed = dict[int:dict]()

    base_data_inside = data["data"]["home"]["home_timeline_urt"]["instructions"]
    data_inside = None

    for each in base_data_inside:
        if each["type"] == "TimelineAddEntries":
            data_inside = each["entries"]
        elif each["type"] == "TimelineShowAlert":
            if each.get("alertType") == "NewTweets":
                pass
            else:
                for user_result in each["usersResults"]:
                    users_data = extract_users_data_timeline(user_result["result"])
                    for user_data in users_data:
                        users_parsed[int(user_data["id"])] = user_data

    for tweet_entry in data_inside:
        if not str(tweet_entry["entryId"]).startswith("tweet-"):
            continue
        print("clientEventInfo", tweet_entry["content"].get("clientEventInfo", {}).get("component"))
        print("socialContext", tweet_entry["content"]["itemContent"].get("socialContext"))
        print(tweet_entry["content"]["itemContent"]["tweetDisplayType"])
        sub_data = tweet_entry["content"]["itemContent"]["tweet_results"]
        if not sub_data:
            continue
        sub_data = sub_data["result"]

        tweet = extract_tweet_data_timeline(sub_data)
        if not tweet:
            continue
        tweets_parsed[int(tweet["id"])] = tweet
        assets_data = extract_assets_data(tweet, int(tweet["id"]))
        for asset_data in assets_data:
            assets_parsed[(asset_data["name"], asset_data["extension"])] = asset_data
        users_data = extract_users_data_timeline(tweet["core"]["user_results"]["result"])
        for user_data in users_data:
            users_parsed[int(user_data["id"])] = user_data

        if tweet.get("quoted_status_result"):
            quote_tweet = tweet["quoted_status_result"]["result"]
            quotetweet = extract_tweet_data_timeline(quote_tweet)
            tweets_parsed[int(quotetweet["id"])] = quotetweet
            assets_data = extract_assets_data(quotetweet.get("legacy"), int(quotetweet["id"]))
            for asset_data in assets_data:
                assets_parsed[(asset_data["name"], asset_data["extension"])] = asset_data
            users_data = extract_users_data_timeline(quotetweet["core"]["user_results"]["result"])
            for user_data in users_data:
                users_parsed[int(user_data["id"])] = user_data

        if tweet.get("retweeted_status_result"):
            raise Exception

        if tweet["legacy"].get("retweeted_status_result"):
            re_tweet = tweet["legacy"]["retweeted_status_result"]["result"]
            retweet = extract_tweet_data_timeline(re_tweet)
            tweets_parsed[int(retweet["id"])] = retweet
            assets_data = extract_assets_data(retweet.get("legacy"), int(retweet["id"]))
            for asset_data in assets_data:
                assets_parsed[(asset_data["name"], asset_data["extension"])] = asset_data
            users_data = extract_users_data_timeline(tweet["core"]["user_results"]["result"])
            for user_data in users_data:
                users_parsed[int(user_data["id"])] = user_data
    return tweets_parsed, assets_parsed, users_parsed


def process_data_legacy(api_data):
    tweets_parsed = dict[int:dict]()
    assets_parsed = dict[tuple[str, str] : dict]()
    users_parsed = dict[int:dict]()

    if "errors" in api_data:
        return {}, {}, {}

    for tweet_entry in api_data:
        tweet_entry: APIOnedotOneHomeEntry
        tweet = extract_tweet_data_legacy(tweet_entry)
        tweets_parsed[int(tweet["id"])] = tweet
        assets_data = extract_assets_data(tweet_entry, int(tweet["id"]))
        for asset_data in assets_data:
            assets_parsed[(asset_data["name"], asset_data["extension"])] = asset_data
        users_data = extract_users_data_legacy(tweet_entry)
        for user_data in users_data:
            users_parsed[int(user_data["id"])] = user_data

        if tweet_entry.get("retweeted_status"):
            retweet = extract_tweet_data_legacy(tweet_entry["retweeted_status"])
            tweets_parsed[int(retweet["id"])] = retweet
            retweet_assets_data = extract_assets_data(tweet_entry["retweeted_status"], int(retweet["id"]))
            for retweet_asset_data in retweet_assets_data:
                assets_parsed[(retweet_asset_data["name"], retweet_asset_data["extension"])] = retweet_asset_data
            users_data = extract_users_data_legacy(tweet_entry["retweeted_status"])
            for user_data in users_data:
                users_parsed[int(user_data["id"])] = user_data
    return tweets_parsed, assets_parsed, users_parsed


async def run():
    db_con: Connection = await asyncpg.connect(**connection_creds)

    get_statement = """
    SELECT id, url, "data", created_at, processed_at
    FROM public.api_dump
    where 
    (
        url like 'https://api.twitter.com/graphql/%/HomeTimeline%'
        or
        url like 'https://api.twitter.com/1.1/statuses/home_timeline.json%'
    )and processed_at is null
    order by id asc limit 10000"""

    for i in range(10000):
        insert_tweets = dict()
        insert_assets = dict()
        insert_users = dict()
        row_ids = list()

        rows = await db_con.fetch(get_statement)
        if not rows:
            break

        for row in rows:
            api_dump_id, url, data, created_at, processed_at = row
            row_ids.append(api_dump_id)
            if data == b"upstream connect error or disconnect/reset before headers. reset reason: remote reset":
                continue
            tweets, assets, users = process_data_url(orjson.loads(data), url)

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

        try:
            values = await db_con.executemany(
                post_insert_statement, [[tweet[var] for var in post_vars] for tweet in insert_tweets.values()]
            )
        except Exception as e:
            print(e)
        print(values)
        values = await db_con.executemany(
            asset_insert_statement, [[asset[var] for var in asset_vars] for asset in insert_assets.values()]
        )
        print(values)
        values = await db_con.executemany(
            user_insert_statement, [[user[var] for var in user_vars] for user in insert_users.values()]
        )
        print(values)

        values = await db_con.execute(api_dump_update_processed, row_ids, datetime.datetime.utcnow())
        print(values)

        print(i)
    await db_con.close()


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    loop.run_until_complete(run())
