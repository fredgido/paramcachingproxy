"""
create table public.api_dump (
	id serial primary key,
	url text not null,
	data bytea not null,
	created_at timestamptz not null,
	processed_at timestamptz null
);
"""


def insert_statement_generator(fields: list[str], table: str, exclude_update=None):
    if exclude_update is None:
        exclude_update = []
    return f"""
INSERT INTO public.{table} ({",".join(f'"{var}"' for var in  fields)})
VALUES ({",".join(f"${i+1}" for i, var in  enumerate(fields))})
ON CONFLICT (id) 
DO UPDATE SET
{",".join(f'"{var}" = EXCLUDED."{var}"' for var in  fields[1:] if var not in exclude_update)}
;"""


"""
create table public.user (
	id bigint primary key,
	created_at timestamptz not null,
	name text not null,
	screen_name text not null,
	location text not null,
	description text not null,
	urls text[] not null,
	protected bool not null,
	followers_count integer null,
	friends_count integer null,
	listed_count integer null,
	statuses_count integer null,
	media_count integer null,
	profile_image_url_https text null,
	profile_banner_url text null,
	profile_background_image_url_https text null,
	processed_at timestamptz null
);
"""


user_vars = [
    "id",
    "created_at",
    "name",
    "screen_name",
    "location",
    "description",
    "urls",
    "protected",
    "followers_count",
    "friends_count",
    "listed_count",
    "statuses_count",
    "media_count",
    "profile_image_url_https",
    "profile_banner_url",
    "processed_at",
]
user_insert_statement = insert_statement_generator(user_vars, "user")


"""
create table public.asset (
	id bigint primary key,
	post_id bigint not null,
	url text not null,
	width smallint not null,
	height smallint not null,
	name text not null,
	extension text not null,
	ext_alt_text text null,
	file_header_date timestamptz null,
	processed_at timestamptz null
);
"""

asset_vars = [
    "id",
    "post_id",
    "url",
    "width",
    "height",
    "name",
    "extension",
    "ext_alt_text",
    "file_header_date",
    "processed_at",
]
asset_insert_statement = insert_statement_generator(asset_vars, "asset", ["file_header_date"])


"""
create table public.post (
	id bigint primary key,
	user_id bigint not null,
	full_text text not null,
	language text not null,
	retweet_count int not null,
	favorite_count int not null,
	reply_count int not null,
    is_quote_status bool not null,
    views int null,
    conversation_id bigint null,
	hashtags text[] not null,
	symbols text[] not null,
	user_mentions bigint[] not null,
	urls text[]  not null,
	is_retweet  bool not null
);
"""

post_vars = [
    "id",
    "user_id",
    "full_text",
    "language",
    "retweet_count",
    "favorite_count",
    "reply_count",
    "is_quote_status",
    "views",
    "conversation_id",
    "hashtags",
    "symbols",
    "user_mentions",
    "urls",
    "is_retweet",
    "processed_at",
]
post_insert_statement = insert_statement_generator(post_vars, "post", ["file_header_date"])

# def insert_statement_generator(fields_updated: list[str], table: str, condition_list_dict,extra_condition=""):
#     quotes= "\""
#     return f"""
# UPDATE {table}
# SET {",".join(f"{quotes}{field}{quotes} = ${i+1}" for i, field in  enumerate(fields_updated))}
# WHERE {" AND ".join(f"{quotes}{field}{quotes} {operator} {value}" for i, (field, (operator,value)) in  enumerate(sorted(condition_list_dict.items(),key=lambda x:x[1] is None)))}
# {"AND" if extra_condition and condition_list_dict else ""} {extra_condition};
# """
#
# print(insert_statement_generator(["processed_at"],"post",{"id":(" in ", "(1,2)")}))


api_dump_update_processed = """
UPDATE public.api_dump
SET "processed_at" = $2
WHERE "id" = any($1::int[]) ;"""
