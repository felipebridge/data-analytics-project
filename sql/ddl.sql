create schema if not exists analytics;

create table if not exists analytics.dim_track (
  track_uri text primary key,
  track_name text,
  artist_name text,
  album_name text
);

create table if not exists analytics.fact_stream (
  stream_id bigserial primary key,
  played_at timestamp not null,
  date_key int not null,
  hour smallint not null,
  platform text,
  track_uri text references analytics.dim_track(track_uri),
  ms_played int not null,
  minutes_played numeric(10,4) not null,
  skipped boolean not null,
  shuffle boolean not null,
  reason_start text,
  reason_end text,
  is_play boolean not null,
  is_effective_play boolean not null,
  is_skip boolean not null
);

create index if not exists ix_fact_date on analytics.fact_stream(date_key);
create index if not exists ix_fact_platform on analytics.fact_stream(platform);
create index if not exists ix_fact_track on analytics.fact_stream(track_uri);