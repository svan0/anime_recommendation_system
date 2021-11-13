bq --location=us-central1 load \
--source_format=CSV \
--allow_jagged_rows \
--skip_leading_rows 1 \
--allow_quoted_newlines \
anime-rec-dev:external_data.user \
gs://anime-rec-dev-data-external/UserList.csv \
username:STRING,\
user_id:STRING,\
user_watching:INTEGER,\
user_completed:INTEGER,\
user_onhold:INTEGER,\
user_dropped:INTEGER,\
user_plantowatch:INTEGER,\
user_days_spent_watching:FLOAT,\
gender:STRING,\
location:STRING,\
birth_date:STRING,\
access_rank:STRING,\
join_date:DATE,\
last_online:DATETIME,\
stats_mean_score:FLOAT,\
stats_rewatched:INTEGER,\
stats_episodes:INTEGER

bq --location=us-central1 load \
--source_format=CSV \
--allow_jagged_rows \
--skip_leading_rows 1 \
--allow_quoted_newlines \
anime-rec-dev:external_data.user_anime \
gs://anime-rec-dev-data-external/UserAnimeList.csv \
username:STRING,\
anime_id:STRING,\
my_watched_episodes:INTEGER,\
my_start_date:STRING,\
my_finish_date:STRING,\
my_score:INTEGER,\
my_status:STRING,\
my_rewatching:STRING,\
my_rewatching_ep:STRING,\
my_last_updated:TIMESTAMP,\
my_tags:STRING