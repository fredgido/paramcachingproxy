"""
create table public.api_dump (
	id serial primary key,
	url text not null,
	data bytea not null,
	created_at timestamptz not null,
	processed_at timestamptz null
);
"""

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
	processed_at timestamptz null
);
"""


"""
create table public.asset (
	id bigint primary key,
	post_id bigint not null,
	url text not null,
	width smallint not null,
	height smallint not null,
	name text not null,
	extension text not null,
	ext_alt_text text not null,
	processed_at timestamptz null
);
"""


"""
create table public.post (
	id bigint primary key,
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


user_insert_vars = (
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
)
user_insert_statement = """
INSERT INTO public.user (
"id", "created_at","name","screen_name","location","description","urls","protected",
"followers_count","friends_count","listed_count","statuses_count","media_count",
"profile_image_url_https","profile_banner_url","processed_at"
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (id) 
DO UPDATE SET
"created_at" = EXCLUDED."created_at",
"name" = EXCLUDED."name",
"screen_name" = EXCLUDED."screen_name",
"location" = EXCLUDED."location",
"description" = EXCLUDED."description",
"urls" = EXCLUDED."urls",
"protected" = EXCLUDED."protected",
"followers_count" = EXCLUDED."followers_count",
"friends_count" = EXCLUDED."friends_count",
"listed_count" = EXCLUDED."listed_count",
"statuses_count" = EXCLUDED."statuses_count",
"media_count" = EXCLUDED."media_count",
"profile_image_url_https" = EXCLUDED."profile_image_url_https",
"profile_banner_url" = EXCLUDED."profile_banner_url",
"processed_at" = EXCLUDED."processed_at"
;"""
