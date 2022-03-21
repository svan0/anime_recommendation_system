"""
    Web service that you can call to crawl specified urls
"""
import logging
import argparse

from crawl_utils import get_item_pipelines
from crawl_utils import run_top_anime_crawler
from crawl_utils import run_recent_profile_crawler
from crawl_utils import run_anime_crawler
from crawl_utils import run_jikan_anime_crawler
from crawl_utils import run_profile_crawler
from crawl_utils import run_jikan_profile_crawler

from scrapy.utils.project import get_project_settings


from flask import Flask, request

from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

logging.basicConfig(
    format='%(levelname)s: %(asctime)s: %(message)s',
    level=logging.INFO
)

global ITEM_PIPELINES
ITEM_PIPELINES = get_item_pipelines('pubsub', 'postgres')

@app.route("/crawl/anime/top", methods = ["POST"])
def crawl_top_anime():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_top_anime_crawler(20, settings)
    return 'Crawled random top anime\n'

@app.route("/crawl/profile/recent", methods = ["POST"])
def crawl_recent_profile():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_recent_profile_crawler(settings)
    return 'Crawled random last online profiles\n'

@app.route("/crawl/anime", methods = ["POST"])
def crawl_anime():
    anime_url = request.get_json()['url']
    anime_urls = anime_url if type(anime_url) == list else [anime_url]
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_anime_crawler(anime_urls, settings)
    return f"Crawled anime\n" 

@app.route("/crawl/anime/jikan", methods = ["POST"])
def jikan_crawl_anime():
    anime_url = request.get_json()['url']
    anime_urls = anime_url if type(anime_url) == list else [anime_url]
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_jikan_anime_crawler(anime_urls, settings)
    return f"Crawled Jikan anime\n" 

@app.route("/crawl/profile", methods = ["POST"])
def crawl_profile():
    profile_url = request.get_json()['url']
    profile_urls = profile_url if type(profile_url) == list else [profile_url]
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_profile_crawler(profile_urls, settings)
    return f"Crawled profile\n"

@app.route("/crawl/profile/jikan", methods = ["POST"])
def jikan_crawl_profile():
    profile_url = request.get_json()['url']
    profile_urls = profile_url if type(profile_url) == list else [profile_url]
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_jikan_profile_crawler(profile_urls, settings)
    return f"Crawled Jikan profile\n"


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--crawled_output', type=str)
    parser.add_argument('--scheduler_db_type', type=str)

    args = parser.parse_args()
    assert(args.crawled_output == 'json' or args.crawled_output == 'pubsub' or args.crawled_output == 'bigquery' or args.crawled_output is None)
    assert(args.scheduler_db_type == 'postgres' or args.scheduler_db_type == 'sqlite' or args.scheduler_db_type is None)

    ITEM_PIPELINES = get_item_pipelines(args.crawled_output, args.scheduler_db_type)
    app.run(host="127.0.0.1", port=9090, debug=True)

