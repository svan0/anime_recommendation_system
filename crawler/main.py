import base64
import logging
import random
import json
import os
from multiprocessing import Process, Queue

from twisted.internet import reactor
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

from flask import Flask, request

from crawler.spiders.anime_spider import AnimeSpider
from crawler.spiders.profile_spider import ProfileSpider
from crawler.spiders.top_anime_spider import TopAnimeSpider
from crawler.spiders.recent_profile_spider import RecentProfileSpider

from dotenv import load_dotenv

app = Flask(__name__)

def local_crawl_top_anime(start_rank, num_pages):
    urls = [
            f'https://myanimelist.net/topanime.php?limit={x}'
            for x in range(start_rank, start_rank + 50 * num_pages, 50)
    ]
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

    process = CrawlerProcess(settings)
    process.crawl(TopAnimeSpider, start_urls = urls)
    process.start()

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

    process = CrawlerProcess(settings)
    process.crawl(RecentProfileSpider)
    process.start()

def local_crawl_anime(anime_url):

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

    process = CrawlerProcess(settings)
    process.crawl(AnimeSpider, start_urls = [anime_url])
    process.start()

def local_crawl_profile(profile_url):

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

    process = CrawlerProcess(settings)
    process.crawl(ProfileSpider, start_urls = [profile_url])
    process.start()



def run_top_anime_crawler(q):
    try:
        start_rank = random.randint(0, 15000)
        num_pages = 20

        urls = [
                f'https://myanimelist.net/topanime.php?limit={x}'
                for x in range(start_rank, start_rank + 50 * num_pages, 50)
        ]
        
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

        runner = CrawlerRunner(settings)
        deferred = runner.crawl(TopAnimeSpider, start_urls = urls)
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run()
        q.put(None)
    
    except Exception as e:
        q.put(e)

def run_recent_profile_crawler(q):
    try:
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
        runner = CrawlerRunner(settings)
        d = runner.crawl(RecentProfileSpider)
        d.addBoth(lambda _: reactor.stop())
        reactor.run()
        q.put(None)
    except Exception as e:
        q.put(e)

def run_anime_crawler(q, anime_url):
    try:
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
        runner = CrawlerRunner(settings)
        d = runner.crawl(AnimeSpider, start_urls = [anime_url])
        d.addBoth(lambda _: reactor.stop())
        reactor.run() 
        q.put(None)
    except Exception as e:
        q.put(e)

def run_profile_crawler(q, profile_url):
    try:
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
        runner = CrawlerRunner(settings)
        d = runner.crawl(ProfileSpider, start_urls = [profile_url])
        d.addBoth(lambda _: reactor.stop())
        reactor.run() 
        q.put(None)
    except Exception as e:
        q.put(e)



@app.route("/crawl_top_anime", methods = ["POST"])
def cloud_crawl_top_anime():
    q = Queue()
    p = Process(target = run_top_anime_crawler, args = (q,))
    p.start()
    p.join()
    result = q.get()
    if result is not None:
        raise result
    return 'Crawled random top anime'

@app.route("/crawl_recent_profile", methods = ["POST"])
def cloud_crawl_recent_profile():
    q = Queue()
    p = Process(target = run_recent_profile_crawler, args = (q,))
    p.start()
    p.join()
    result = q.get()
    if result is not None:
        raise result
    return 'Crawled random last online profiles'

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
    
    q = Queue()
    p = Process(target = run_anime_crawler, args = (q, anime_url, ))
    p.start()
    p.join()
    result = q.get()
    if result is not None:
        raise result
    
    return f"Crawled anime {anime_url}"

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
    
    q = Queue()
    p = Process(target = run_profile_crawler, args = (q, profile_url))
    p.start()
    p.join()
    result = q.get()
    if result is not None:
        raise result
    
    return f"Crawled anime {profile_url}"

if __name__ == '__main__':
    load_dotenv()
    app.run(host="127.0.0.1", port=9090, debug=True)

    """
    anime_url_message = 'https://myanimelist.net/anime/48926/Komi-san_wa_Comyushou_desu'
    anime_url_message = anime_url_message.encode("utf-8")
    anime_url_message = base64.b64encode(anime_url_message)
    event = {'data' : anime_url_message}
    cloud_crawl_anime(event = event, context = None)

    profile_url_message = 'https://myanimelist.net/profile/RealKurapikaTard'
    profile_url_message = profile_url_message.encode("utf-8")
    profile_url_message = base64.b64encode(profile_url_message)
    event = {'data' : profile_url_message}
    cloud_crawl_profile(event = event, context = None)
    """
