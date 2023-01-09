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
	url text[] not null,
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
