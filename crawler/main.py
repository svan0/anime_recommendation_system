import base64
import logging

from crawler.utils import GCP_PIPELINES, LOCAL_PIPELINES
from crawler.utils import run_top_anime_crawler, run_recent_profile_crawler, run_anime_crawler, run_profile_crawler

from scrapy.utils.project import get_project_settings


from flask import Flask, request

from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.logger.setLevel(logging.INFO)


@app.route("/local_crawl_top_anime", methods = ["POST"])
def local_crawl_top_anime():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = LOCAL_PIPELINES
    run_top_anime_crawler(2, settings)
    return '(Local) Crawled random top anime\n'

@app.route("/local_crawl_recent_profile", methods = ["POST"])
def local_crawl_recent_profile():

    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = LOCAL_PIPELINES
    run_recent_profile_crawler(settings)
    return '(Local) Crawled random last online profiles\n'

@app.route("/local_crawl_anime", methods = ["POST"])
def local_crawl_anime():

    anime_url = request.get_json()['url']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = LOCAL_PIPELINES

    run_anime_crawler([anime_url], settings)

    return f"(Local) crawled anime {anime_url}\n" 

@app.route("/local_crawl_profile", methods = ["POST"])
def local_crawl_profile():

    profile_url = request.get_json()['url']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = LOCAL_PIPELINES

    run_profile_crawler([profile_url], settings)
    return f"(Local) crawled profile {profile_url}\n"


@app.route("/crawl_top_anime", methods = ["POST"])
def cloud_crawl_top_anime():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = GCP_PIPELINES
    run_top_anime_crawler(20, settings)
    return 'Crawled random top anime\n'

@app.route("/crawl_recent_profile", methods = ["POST"])
def cloud_crawl_recent_profile():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = GCP_PIPELINES

    run_recent_profile_crawler(settings)
    return 'Crawled random last online profiles\n'

@app.route("/crawl_anime", methods = ["POST"])
def cloud_crawl_anime():

    anime_url = request.get_json()['url']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = GCP_PIPELINES

    run_anime_crawler([anime_url], settings)

    return f"Crawled anime {anime_url}\n"

@app.route("/crawl_profile", methods = ["POST"])
def cloud_crawl_profile():
    
    profile_url = request.get_json()['url']
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = GCP_PIPELINES
    
    run_profile_crawler([profile_url], settings)
    
    return f"Crawled profile {profile_url}\n"

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=9090, debug=True)

