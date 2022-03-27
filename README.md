# Anime Recommendation System 
## Motivation
I have enjoyed watching anime ever since I was kid and I'll probably keep on enjoying anime for the rest of my life. <br /> However as I am going through the animes on my todo list, it is becoming harder and harder to find what to watch next. <br />I am sure there are lots of undiscovered gems that I would love to watch but how should I go about discovering them? <br /> One day I spent about an hour going through myanimelist.net searching for something to watch. Looking at how time consuming it was for me and probably for others, I decided to create an anime recommendation system that would recommend what to watch next for as many myanimelist users as possible.
<br /> <br />
At the time I was also preparing for the Google Cloud Professional Machine Learning Certifcation, so I decided that it would make sense to use as many different GCP services as possible for learning purposes. (I passed the certification BTW, thank you for asking :P )

## Goals
When I started this project I had a couple of ambitious goals:
- Create an end to end system.
- The system would continuously and intelligently crawl myanimelist.net for new data.
- The system would run an hourly ETL pipeline on the crawled data to generate clean and validated datasets.
- The system would run kubeflow based ML pipelines on the generated data to generate batch recommendations for the MAL users.
- The system would expose the recommendations through a web application.
- The system needs to easily scale on the cloud.
- The system would be implemented using good software, data and machine learning engineering practices.
- Crawl data for at least 1 million myanimelist users.
- Generate clean datasets and share them with the Kaggle community.
- Generate recommendations for at least 500K myanimelist users.
- Get at least 100 users to try it out in the first couple of weeks of releasing the web app and get their feedback.

## Non-goals
- Extensive experiementation with various machine learning algorithms and feature engineering techniques in order to get the best possible recommendations is not a goal of this project. 
- This could change with the help of the Kaggle community. Kagglers can experiment with the dataset and open PRs to integrate their ML algorithms to the ML pipelines and serve them on the web app.

## Where can I try it out
You can try the Anime Recommendation System at this link https://anime-rec-dev.uc.r.appspot.com/ <br />
When a user enters the web app, it will be asked to enter its MAL id
![Web app 1](images/web_app_1.png)
Once that is done the app will show the user 5 random animes from the top 20 animes that we recommend for that user.
![Web app 2](images/web_app_2.png)

## Architecture Design and components
### System design overview
![System Design Pic](images/system_design.png)
The project is composed of 5 main parts:
- Crawl scheduler: A job that when called will fetch a list of anime or profile urls from a database and push them to a message queue.
The crawl schedulers are deployed as Cloud Functions, the database as postgres Cloud SQL db and the message queue as a Pub/Sub topic.
- Crawler: Scrapy jobs that that fetch message urls from the scheduler message queue and crawl them. The crawled data items are pushed to a data ingestion message queue and the crawler jobs also connect to the schedule database to update it. The crawler is deployed to Google Kubernetes Engine and the data ingestion queue is a Pub/Sub topic.
- Data Ingestion and ETL: An Apache Beam streaming job pulls data items from the ingestion queue and pushes them to BigQuery in a landing area. An Apache Airflow pipeline is run to aggregate, clean and validate the crawled data into well structured datasets. The new data is saved in BigQuery and Storage.
The ingestion beam job is deployed as a Dataflow job and the Apache Airflow ETL pipeline runs in a Cloud Composer environment.
- Machine Learning Pipelines: Each pipeline handles both retrieval and ranking steps. <br /> For retrieval step, the pipeline starts by generating the train/val/test data for retrieval, then trains a retrieval model and finally runs batch inference for retrieval and saves the results. <br /> For ranking step, the pipeline starts by generating the train/val/test data for ranking, then trains a ranking model and finally runs batch inference for ranking on the retrieved results and saves the final results. <br /> 
The pipelines are Kubeflow pipelines and they run on Google VertexAI pipelines. Data is fetched from and saved to both BigQuery and Storage.
- Web App: The generated recommendations are ingested into a Redis database and a small flask web application fetches the recommendations from Redis for each user recommendation request. 

The number steps in the above diagram are:
- 1: A Cloud Scheduler cron job triggers the crawl scheduler Cloud Functions
- 2: The crawl scheduler Cloud Function fetches the urls to schedule from the Scheduler Cloud SQL database.
- 3: The crawl scheduler Cloud Function pushes the urls to crawl to the PubSub schedule queue.
- 4: The crawl workers running on Google Kubernetes Engine pull the urls from the PubSub schedule queue.
- 5: The craw workers crawl the urls, update the scheduler database and push the crawled data to the PubSub data ingestion queue.
- 6: The Dataflow ingestion job pulls the data from the data ingestion queue.
- 7: The Dataflow ingestion job pushes the data to BigQuery landing area.
- 8: Cloud Composer ETL pipeline aggregates, merges, cleans and validates the data from the landing area.
- 9: Cloud Composer ETL pipeline saves the cleaned datasets to BigQuery processed area.
- 10: Vertex AI ML pipelines use the data in the BigQuery processed area to train ML models and generate batch recommendations.
- 11: Batch recommendations are duplicated in Cloud Memorystore Redis instance for low latency access.
- 12: The web application access the Cloud Memorystore Redis instance to fetch the recommendations for each user.
- 13: User sends a request to the web app and knows which anime to watch.

### Crawl scheduler
The scheduler database contains two tables `anime_schedule` and `profile_schedule`. 
Both tables have the same schema:
```
    url VARCHAR(255) PRIMARY KEY NOT NULL,
    last_scheduled_date TIMESTAMP,
    scheduled_count INT,
    last_crawled_date TIMESTAMP,
    crawled_count INT,
    last_inspected_date TIMESTAMP,
    inspected_count INT
```
- `url` is the anime/profile url on myanimelist.
- `last_scheduled_date` is the datetime of the last time the anime/profile was pushed to the schedule queue. 
- `scheduled_count` is the number of times the anime/profile was pushed to the schedule queue. 
- `last_crawled_date` is the datetime of the last time the anime/profile data item was pushed to the data ingestion queue.
- `scheduled_count` is the number of times the anime/profile data item was pushed to the data ingestion queue. 
- `last_inspected_date` is the datetime of the last time the anime/profile was inspected when crawling another anime/profile.
- `inspected_count` is the number of times the anime/profile was inspected when crawling another anime/profile.

When we call the scheduler to get the list of anime/profiles to crawl, it first returns those that have never been scheduled, then those that were last crawled a long time ago, then those that have been inspected not a long time. <br /> It also discards the anime/profiles that were scheduled not long ago but weren't crawled. There probably is an issue when crawling those anime/profiles and we don't want them to always be returned whenever we want to schedule animes/profiles.

### Crawler
We have anime and profile crawlers that can either crawl [myanimelist.net](https://myanimelist.net/) or the [Jikan API](https://docs.api.jikan.moe/). <br /> 
When we crawl an anime, we scrape:
- the anime info (title, score, synopsis, etc...)
- the anime's related animes (sequels, prequels, etc...) and update the "inspected" fields for the related anime in the scheduler db
- the animes that MAL users recommend for that anime and update the "inspected" fields for the recommended anime in the scheduler db
- the reviews for that anime and update the "inspected" fields for the users that reviewed it in the scheduler db
When we crawl a user, we scrape:
- the user's info (last online date, mean score etc...)
- the user's watch list (animes completed, plan to watch, watching etc...) and update the "inspected" fields for the animes in the scheduler db
- the user's latest activity (latest completed, watched anime) and update the "inspected" fields for the animes in the scheduler db
- the user's favorite animes and update the "inspected" fields for the animes in the scheduler db
- the user's friends and update the "inspected" fields for the new users in the scheduler db

The crawler can be run as crawl jobs that pull messages from the schedule queue and crawl the anime/profile urls in the pulled messages. <br />
The crawler can also be run as a web service where you can issue requests for one or a list of animes/profile urls to be crawled. <br />
We can also issue a request to the crawl app to scrape a random list of anime url or profile urls from myanimelist and only update the scheduler DB. This will be used to start populating the scheduler DB (initial seed).

<br /> 
The crawlers can be configured to save the scraped data either locally in JSON files, directly to the BigQuery landing table or to a PubSub topic.

### Data Ingestion and ETL
When data is crawled, it is pushed to a PubSub topic. We then have a Dataflow job that pulls data from the PubSub topic and appends it to the corresponding BigQuery tables in a "landing area" (a BigQuery dataset). For more details on the landing tables schemas, see `etl/landing_area_schemas/`

<br /> 

Landing area is not clean. We could have duplicates (animes and profiles crawled more than once), inconsistencies, etc... We need to run an ETL pipeline that aggregates, merges and cleans up the data and saves it in a "staging area". After that the pipeline validates the data in the "staging area" and if it is valid, moves it to the "processed area".

Below is a description of the schema of the tables in the processed area:
#### Anime:
- anime_id: The id of the anime
- anime_url: The myanimelist url of the anime
- title: The name of the anime
- synopsis: Short description of the plot of the anime
- main_pic: Url to the cover picture of the anime
- type: Type of the anime (example TV, Movie, OVA etc...)
- source_type: Type of the source of the anime (example Manga, Light Novel etc..)
- num_episodes: Number of episodes in the anime
- status: The current status of the anime (Finished airing, Currently airing or Not yet aired)
- start_date: Start date of the anime
- end_date: End date of the anime
- season: Season the anime started airing on (example animes that started in Jan 2020 have season Winter 2020)
- studios: List of studios that created the anime
- genres: List of the anime genres (Action, Shonen etc..)
- scoore: Average score of the anime on myanimelist
- score_count: Number of users that scored the anime
- score_rank: Rank of anime based on its score on myanimelist
- popularity_rank: Rank of anime based on its popularity on myanimelist
- members_count: Number of users that are members of the anime
- favorites_count: Number of users that have the anime as a favorite anime
- watching_count: Number of users watching the anime
- completed_count: Number of users that have completed the anime
- on_hold_count: Number of users that have the anime on hold
- dropped_count: Number of users that have dropped the anime
- plan_to_watch_count: Number of users that plan to watch the anime
- total_count: Total number of users that either completed, plan to watch, are watching, dropped or have the anime on hold
- score_10_count: Number of users that score the anime a 10
- score_09_count: Number of users that score the anime a 9
- score_08_count: Number of users that score the anime a 8
- score_07_count: Number of users that score the anime a 7
- score_06_count: Number of users that score the anime a 6
- score_05_count: Number of users that score the anime a 5
- score_04_count: Number of users that score the anime a 4
- score_03_count: Number of users that score the anime a 3
- score_02_count: Number of users that score the anime a 2
- score_01_count: Number of users that score the anime a 1
- clubs: List of MAL clubs the anime is part of
- pics: List of urls too pictures of the anime

#### User
- user_id: The id of the user
- user_url: The url of the user on myanimelist
- last_online_date: Datetime of the last time the user logged into myanimelist.net
- num_watching: Number of animes the user is watching
- num_completed: Number of animes the user completed
- num_on_hold: Number of animes the user has on hold
- num_dropped: Number of animes the user has dropped
- num_plan_to_watch: Number of animes the user plans to watch
- num_days: Number of days the user has spent watching anime
- mean_score: Mean score the user has given to animes
- clubs: List of MAL clubs the user is member of


#### User Anime
Table contains relationships between user and animes
- user_id: The id of the user
- anime_id: The id of the anime
- favorite: 0 or 1 depending if anime_id is a favorite anime of user_id
- review_id: Id of the review if user_id reviewed anime_id
- review_date: Date the review was made
- review_num_useful: Number of users that found the review useful
- review_score: Overall score for the anime given in the review
- review_story_score: Story score for the anime given in the review
- review_animation_score: Animation score for the anime given in the review
- review_sound_score: Sound score for the anime given in the review
- review_character_score: Character score for the anime given in the review
- review_enjoyment_score: Enjoyment score for the anime given in the review
- score: Score the user has given to the anime (does not need to have given a review)
- status: Has the user "completed", "watching", "plan_to_watch", "dropped", "on_hold" the anime
- progress: Number of episodes the user has watched
- last_interaction_date: Last datetime the user has interacted with this anime

#### Anime Anime
Table contains relationships between pairs of animes
- animeA: The id of the first anime
- animeB: The id of the second anime
- recommendation: 0 or 1 depending if animeB is a recommendation of animeA
- recommendation_url: Url of the recommendation if animeB is a recommendation of animeA
- num_recommenders: Number of users that recommend animeB for animeA
- related: 0 or 1 depending if animeB is related to animeA
- relation_type: The type of relation between related animes (Sequel, Prequel etc...)

#### User User
Table contains relationships between pairs of users
- userA: The id of the first user
- userB: The id of the second user
- friends: 0 or 1 depending if userA and userB are friends
- friendship_date: Datetime that userA and userB became friends 

### ML pipelines
We currently have two type of approaches to recommend animes to users:
- Anime Anime recommendation: For each distinct anime we recommend a list of animes to watch next. Then for each user, based on the last anime it watched we recommend the animes recommended for that anime.
- User Anime recommendation: For each user we recommend a list of anime to watch next.

Below are some details on how the two approaches that are currently implemented:
#### Anime Anime
##### Retrieval step
![Anime Anime Retrieval](images/anime_anime_retrieval.png)
For each animeA we select animeB that are either 
- related
- have the most number of users that recommend them on MAL
- have high cosine similarity of the genres
- are co_completed by many users in a short period of time

We use these pairs of animes to train a Tensorflow Recommenders Retrieval model.
We only use the anime ids as a features and pass them through an embedding layers. Experiemtation is needed to decide on which features to add and might be the focus of subsequent iterations on this project.
Once the model is trained we run batch inference and retrieve the top 300 animeB for each animeA.

##### Ranking step
![Anime Anime Ranking](images/anime_anime_ranking.png)
We train a model that takes an anchor anime_id and a pair of retrieved animes and scores the more relevant one higher.
We then use the trained scoring module to score and rank animes.
The train/val/test dataset has 4 fields anime_id, retrieved_anime_id_1, retrieved_anime_id_2 and label. 
- anime_id is the anchor anime
- retrieved_anime_id_1 is the first retrieved anime
- retrieved_anime_id_2 is the second retrieved anime. 
- If label = 1 then retrieved_anime_id_1 is more relevant to anime_id than retrieved_anime_id_2. 
- If label = 0 then retrieved_anime_id_2 is more relevant to anime_id than retrieved_anime_id_2.

During training, the model will return score_1 for (anime, retrieved_anime_id_1) and score_2 for (anime_id, retrieved_anime_id_2). <br /> If label = 1 we want score_1 > score_2 else we want score_1 < score_2.

We only use the anime ids as a features and pass them through an embedding layers. Experiemtation is needed to decide on which features to add and might be the focus of subsequent iterations on this project.

Once the model is trained, we iterate over all pairs of animes that we generated during the inference step of the retrieval model and get a score which will be used to rank the retrieved animes.

Now that we have ranked anime pairs, we can get ranked animes for each user based on the last anime it completed.

#### User Anime
##### Retrieval step
![User Anime Retrieval](images/user_anime_retrieval.png)
For each user_id we select anime_id that the user completed. <br />
We use these (user_id, anime_id) pairs to train a Tensorflow Recommenders Retrieval model. <br />
We only use the user ids and anime ids as a features and pass them through an embedding layers. Experiemtation is needed to decide on which features to add and might be the focus of subsequent iterations on this project. <br />
Once the model is trained we run batch inference and retrieve the top 300 animes for each user_id.

##### Ranking step
![User Anime Ranking](images/user_anime_ranking.png)
There are 2 approaches to train the user anime ranking model:
- Ranking approach: We train a model that given (user_id, anime_id) tries to predict the score and minimize the MSE loss.
- List ranking approach: We train a model that given (user_id, list of anime_ids) tries to predict the score for each anime and minimize the ListMLELoss using Tensorflow Recommenders Ranking.

Once the model is trained, we iterate over all pairs of (user, anime) that we generated during the inference step of the retrieval model and get a score which will be used to rank the retrieved animes.

### Web app
The generated recommendation are saved to a Redis database for low latency access.
When a user enters the web app, it will be asked to enter its MAL id
![Web app 1](images/web_app_1.png)
Once that is done the app will show the user 5 random animes from the top 20 animes that we recommend for that user.
![Web app 2](images/web_app_2.png)

## Steps to run
### Step 1: Setup local .env file
First step is to create at the root of this project a .env file and fill it as follows:
```
PROJECT_ID=<your-gcp-project-id>
GOOGLE_APPLICATION_CREDENTIALS=<your-local-gcp-credentials-file>
SERVICE_ACCOUNT=<here-I-use-the-name-of-project-owner-service-account>

# [CRAWLER]
CRAWLER_REGION=<gcp-region-you-want-to-run-crawler-on>

ANIME_SCHEDULE_CRON_JOB_NAME=<name-of-anime-scheduler-cron-job>
ANIME_SCHEDULE_CLOUD_FUNCTION_NAME=<name-of-anime-scheduler-cloud-function>
PROFILE_SCHEDULE_CRON_JOB_NAME=<name-of-profile-scheduler-cron-job>
PROFILE_SCHEDULE_CLOUD_FUNCTION_NAME=<name-of-profile-scheduler-cloud-function>

CRAWLER_CLUSTER_ZONE="${CRAWLER_REGION}-a"
CRAWLER_CLUSTER_NAME="crawler-cluster-${CRAWLER_REGION}"

SCHEDULER_DB_INSTANCE_NAME=<name-of-scheduler-db-instance> 
SCHEDULER_DB_INSTANCE="${PROJECT_ID}:${CRAWLER_REGION}:${SCHEDULER_DB_INSTANCE_NAME}"
SCHEDULER_DB_HOST="127.0.0.1" #We use a proxy to connect to the db from the crawler so host is localhost
SCHEDULER_DB=<name-of-scheduler-database>
SCHEDULER_DB_USER=<name-of-scheduler-database-user>
SCHEDULER_DB_PASSWORD=<scheduler-database-user-password>

SCHEDULE_ANIME_PUBSUB_TOPIC=<name-of-anime-schedule-pubsub-topic>
SCHEDULE_ANIME_PUBSUB_SUBSCRIPTION=<name-of-anime-schedule-pubsub-subscription>
SCHEDULE_PROFILE_PUBSUB_TOPIC=<name-of-profile-schedule-pubsub-topic>
SCHEDULE_PROFILE_PUBSUB_SUBSCRIPTION=<name-of-profile-schedule-pubsub-subscription>

# [ETL]
ETL_REGION=<gcp-region-you-want-to-run-ingestion-and-etl-on>
DATA_INGESTION_PUBSUB_TOPIC=<name-of-data-ingestion-pubsub-topic>
DATAFLOW_GCS_BUCKET=<gcs-bucket-path-to-save-dataflow-temp-data>
DATA_INGESTON_JOB_ID=<dataflow-job-name>
COMPOSER_ENV_NAME=<composer-environment-name>

# [WEB APP]
WEB_APP_REGION=<gcp-region-you-want-to-run-web-app-on>
WEB_APP_REDIS_INSTANCE_ID=<web-app-redis-instance-name>
WEB_APP_REDIS_VPC_CONNECTOR=<web-app-redis-vpc-connector-name>
WEB_APP_DATAFLOW_JOB_ID=<click-stream-data-dataflow-ingestion-name>
WEB_APP_PUBSUB_TOPIC=<click-stream-data-pubsub-topic>
WEB_APP_BQ_DATASET_ID=<name-of-bigquery-dataset-to-save-click-stream-data>
WEB_APP_BQ_TABLE_ID=<name-of-bigquery-table-to-save-click-stream-data>
```
Here's the example of what I have (for quick copy paste)
```
PROJECT_ID=<your gcp project id>
GOOGLE_APPLICATION_CREDENTIALS=<your-local-gcp-credentials-file>
SERVICE_ACCOUNT=<here-I-use-the-name-of-project-owner-service-account>

# [CRAWLER]
CRAWLER_REGION="us-west1"

ANIME_SCHEDULE_CRON_JOB_NAME="anime_crawl_scheduler"
ANIME_SCHEDULE_CLOUD_FUNCTION_NAME="anime_crawl_scheduler"
PROFILE_SCHEDULE_CRON_JOB_NAME="profile_crawl_scheduler"
PROFILE_SCHEDULE_CLOUD_FUNCTION_NAME="profile_crawl_scheduler"

CRAWLER_CLUSTER_ZONE="${CRAWLER_REGION}-a"
CRAWLER_CLUSTER_NAME="crawler-cluster-${CRAWLER_REGION}"

SCHEDULER_DB_INSTANCE_NAME="scheduler-db-instance-dev"
SCHEDULER_DB_INSTANCE="${PROJECT_ID}:${CRAWLER_REGION}:${SCHEDULER_DB_INSTANCE_NAME}"
SCHEDULER_DB_HOST="127.0.0.1"
SCHEDULER_DB="scheduler_db"
SCHEDULER_DB_USER="postgres"
SCHEDULER_DB_PASSWORD="password"

SCHEDULE_ANIME_PUBSUB_TOPIC="anime_crawl_queue"
SCHEDULE_ANIME_PUBSUB_SUBSCRIPTION="anime_crawl_subscription"
SCHEDULE_PROFILE_PUBSUB_TOPIC="profile_crawl_queue"
SCHEDULE_PROFILE_PUBSUB_SUBSCRIPTION="profile_crawl_subscription"

# [ETL]
ETL_REGION="us-west1"
DATA_INGESTION_PUBSUB_TOPIC="data_ingestion_queue"
DATAFLOW_GCS_BUCKET="gs://${PROJECT_ID}-dataflow-temp"
DATA_INGESTON_JOB_ID="data_ingestion"
COMPOSER_ENV_NAME="anime-etl-composer-env"

# [WEB APP]
WEB_APP_REGION="us-central1"
WEB_APP_REDIS_INSTANCE_ID="anime-rec-dev-web-app-redis"
WEB_APP_REDIS_VPC_CONNECTOR="redis-vpc-connector"
WEB_APP_DATAFLOW_JOB_ID="web_app_click_stream_ingest"
WEB_APP_PUBSUB_TOPIC="web_app_click_stream_topic"
WEB_APP_BQ_DATASET_ID="web_app"
WEB_APP_BQ_TABLE_ID="click_data"
```
### Step 2: Setup cloud function and web app env_variables.yaml files
Create the file crawl_scheduler/env_variables.yaml and fill it as follows with the same values defined in the .env file:
```
PROJECT_ID: <your-value>
SCHEDULER_DB_INSTANCE: <your-value>
SCHEDULER_DB: <your-value>
SCHEDULER_DB_USER: <your-value>
SCHEDULER_DB_PASSWORD: <your-value>
SCHEDULE_ANIME_PUBSUB_TOPIC: <your-value>
SCHEDULE_PROFILE_PUBSUB_TOPIC: <your-value>
```
Create the file web_app/app/env_variables.yaml and fill it as follows with the same values defined in the .env file:
```
env_variables:
    PROJECT_ID: <your-value>
    PUBSUB_TOPIC: <your-value>
    REDIS_INSTANCE_HOST: <you-can-find-it-in-the-gcp-console>
    REDIS_INSTANCE_PORT: "6379"
```
### Step 3: Update kubernetes and kubeflow manifest files
Change the docker image URI in the files in this folder crawler/manifests. Changing the project name is enough.
Change the docker image URI in the files in ml_pipelines/pipelines/components/*/*/component.yaml. Changing the project name is enough.
### Step 4: Run the scripts
#### Crawler
Run these scripts in order:
- crawler/scripts/step_1_setup_pubsub.sh
- crawler/scripts/step_2_setup_cloud_sql_instance.sh
- crawler/scripts/step_3_setup_crawler_cluster.sh
- crawler/scripts/step_4_build_docker.sh
- crawler/scripts/step_5_deploy_crawler.sh
You should have a service running in Kubernetes engine. Trigger the service with a POST and empty payload at this route /crawl/anime/top.
This will seed the scheduler DB, which is currently empty.

#### Crawl scheduler
Run these scripts in order:
- crawl_scheduler/scripts/step_1_setup_crawl_queues.sh
- crawl_scheduler/scripts/step_2_deploy_cloud_functions.sh
- crawl_scheduler/scripts/step_3_setup_cloud_scheduler.sh

#### ETL
Run these scripts in order:
- etl/scripts/step_0_setup_bq.sh
- etl/scripts/step_1a_prepare_dataflow_ingestion_template.sh
- etl/scripts/step_1b_start_dataflow_ingestion_job.sh
- etl/scripts/step_2a_create_composer_env.sh
- etl/scripts/step_2b_import_dag_composer.sh
- etl/scripts/step_2c_trigger_etl_dag.sh (also whenever you want to rerun the ETL pipeline)

#### ML Pipelines
Run these scripts in order:
- ml_pipelines/scripts/step_1_build_docker.sh
- ml_pipelines/scripts/step_2_run_pipelines.sh (also whenever you want to rerun the ML pipelines)

#### Web app:
Run these scripts in order:
- web_app/scripts/step_1_setup_redis.sh
- web_app/scripts/step_2_setup_click_data_pipeline.sh
- web_app/scripts/step_3_deploy.sh

## Future work
- Create a good looking UI for the web app
- Change infrasture scripts to terraform
- Set up CI/CD and CT workflows

## Acknowledgements
I would like to appreciate MyAnimeList.net for the great platform and JikanAPI for the great anime API they provide.
