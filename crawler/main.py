import base64
import logging
import random
import os
import crochet
crochet.setup()

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from flask import Flask, request

from crawler.spiders.anime_spider import AnimeSpider
from crawler.spiders.profile_spider import ProfileSpider
from crawler.spiders.top_anime_spider import TopAnimeSpider
from crawler.spiders.recent_profile_spider import RecentProfileSpider

from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

"""
def init_connection_engine() -> sqlalchemy.engine.Engine:
    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            os.environ["SCHEDULER_DB_INSTANCE"],
            "pg8000",
            user=os.environ["SCHEDULER_DB_USER"],
            password=os.environ["SCHEDULER_DB_PASSWORD"],
            db=os.environ["SCHEDULER_DB"],
        )
        return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return engine

#db_conn = init_connection_engine().connect()
"""

@crochet.wait_for(timeout = 300)
def run_top_anime_crawler(num_pages, crawler_settings):
    start_rank = random.randint(0, 15000)

    urls = [
            f'https://myanimelist.net/topanime.php?limit={x}'
            for x in range(start_rank, start_rank + 50 * num_pages, 50)
    ]
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(TopAnimeSpider, start_urls = urls)
    return deferred

@crochet.wait_for(timeout = 60)
def run_recent_profile_crawler(crawler_settings):
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(RecentProfileSpider)
    return deferred

@crochet.wait_for(timeout = 500)
def run_anime_crawler(anime_url, crawler_settings):
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(AnimeSpider, start_urls = [anime_url])
    return deferred

@crochet.wait_for(timeout = 500)
def run_profile_crawler(profile_url, crawler_settings):
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(ProfileSpider, start_urls = [profile_url])
    return deferred


@app.route("/local_crawl_top_anime", methods = ["POST"])
def local_crawl_top_anime():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }
    run_top_anime_crawler(2, settings)
    return '(Local) Crawled random top anime\n'

@app.route("/local_crawl_recent_profile", methods = ["POST"])
def local_crawl_recent_profile():

    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }
    run_recent_profile_crawler(settings)
    return '(Local) Crawled random last online profiles\n'

@app.route("/local_crawl_anime", methods = ["POST"])
def local_crawl_anime():
    
    envelope = request.get_json()
    if not envelope:
        logging.error("no Pub/Sub message received")
        return "Bad request: no Pub/Sub message received", 400
    
    if not isinstance(envelope, dict) or "message" not in envelope:
        logging.error("invalid message received")
        return "Bad request: invalid message received", 400
    
    message = envelope['message']
    
    try:
        anime_url = base64.b64decode(message['data']).decode('utf-8')
    except:
        anime_url = message['data']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }

    run_anime_crawler(anime_url, settings)

    return f"(Local) crawled anime {anime_url}\n" 

@app.route("/local_crawl_profile", methods = ["POST"])
def local_crawl_profile():

    envelope = request.get_json()
    if not envelope:
        logging.error("no Pub/Sub message received")
        return "Bad request: no Pub/Sub message received", 400
    
    if not isinstance(envelope, dict) or "message" not in envelope:
        logging.error("invalid message received")
        return "Bad request: invalid message received", 400
    
    message = envelope['message']

    try:
        profile_url = base64.b64decode(message['data']).decode('utf-8')
    except:
        profile_url = message['data']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }

    run_profile_crawler(profile_url, settings)
    return f"(Local) crawled profile {profile_url}\n"


@app.route("/crawl_top_anime", methods = ["POST"])
def cloud_crawl_top_anime():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 50
    }
    run_top_anime_crawler(20, settings)
    return 'Crawled random top anime\n'

@app.route("/crawl_recent_profile", methods = ["POST"])
def cloud_crawl_recent_profile():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 50
    }
    run_recent_profile_crawler(settings)
    return 'Crawled random last online profiles\n'

@app.route("/crawl_anime", methods = ["POST"])
def cloud_crawl_anime():
    envelope = request.get_json()
    if not envelope:
        logging.error("no Pub/Sub message received")
        return "Bad request: no Pub/Sub message received", 400
    
    if not isinstance(envelope, dict) or "message" not in envelope:
        logging.error("invalid message received")
        return "Bad request: invalid message received", 400
    
    message = envelope['message']
    
    try:
        anime_url = base64.b64decode(message['data']).decode('utf-8')
    except:
        anime_url = message['data']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 100,
        'crawler.pipelines.pub_sub_pipeline.PubSubPipeline' : 300,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 500
    }

    print(f"ANIME {anime_url}")
    run_anime_crawler(anime_url, settings)

    return f"Crawled anime {anime_url}\n"

@app.route("/crawl_profile", methods = ["POST"])
def cloud_crawl_profile():
    
    envelope = request.get_json()
    if not envelope:
        logging.error("no Pub/Sub message received")
        return "Bad request: no Pub/Sub message received", 400
    
    if not isinstance(envelope, dict) or "message" not in envelope:
        logging.error("invalid message received")
        return "Bad request: invalid message received", 400
    
    message = envelope['message']

    try:
        profile_url = base64.b64decode(message['data']).decode('utf-8')
    except:
        profile_url = message['data']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 100,
        'crawler.pipelines.pub_sub_pipeline.PubSubPipeline' : 300,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 500
    }
    
    run_profile_crawler(profile_url, settings)
    
    return f"Crawled profile {profile_url}\n"

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=9090, debug=True)

