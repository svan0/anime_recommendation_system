"""
    Crawl worker that fetches urls from PubSub and runs crawling jobs
"""
import argparse
import os
from random import random
import time
import logging
import random

from dotenv import load_dotenv
import crochet
crochet.setup()

from google.api_core import retry
from google.cloud import pubsub

from scrapy.utils.project import get_project_settings

from crawl_utils import get_item_pipelines
from crawl_utils import run_anime_crawler
from crawl_utils import run_jikan_anime_crawler
from crawl_utils import run_profile_crawler
from crawl_utils import run_jikan_profile_crawler

load_dotenv()

logging.basicConfig(
    format='%(levelname)s: %(asctime)s: %(message)s',
    level=logging.INFO
)

def crawl_anime(anime_urls):
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_anime_crawler(anime_urls, settings)

def jikan_crawl_anime(anime_urls):
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_jikan_anime_crawler(anime_urls, settings)

def crawl_profile(profile_urls):
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_profile_crawler(profile_urls, settings)

def jikan_crawl_profile(profile_urls):
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = ITEM_PIPELINES
    run_jikan_profile_crawler(profile_urls, settings)


if __name__ == '__main__':
    global ITEM_PIPELINES

    parser = argparse.ArgumentParser()
    parser.add_argument('--anime', dest='anime', action='store_true')
    parser.add_argument('--profile', dest='profile', action='store_true')
    parser.add_argument('--crawled_output', type=str)
    parser.add_argument('--scheduler_db_type', type=str)
    parser.add_argument('--jikan', dest='jikan', action='store_true')
    parser.add_argument('--max_urls', type=int)
    parser.set_defaults(anime = False)
    parser.set_defaults(profile = False)
    parser.set_defaults(max_urls = 100)
    args = parser.parse_args()

    assert(args.crawled_output == 'json' or args.crawled_output == 'pubsub' or args.crawled_output == 'bigquery' or args.crawled_output == 'all' or args.crawled_output is None)
    assert(args.scheduler_db_type == 'postgres' or args.scheduler_db_type == 'sqlite' or args.scheduler_db_type is None)

    ITEM_PIPELINES = get_item_pipelines(args.crawled_output, args.scheduler_db_type)

    assert(args.anime or args.profile)
    assert(not(args.anime and args.profile))

    project_id = os.getenv("PROJECT_ID")

    if args.anime:
        subscription_name = os.getenv("SCHEDULE_ANIME_PUBSUB_SUBSCRIPTION")
        if args.jikan:
            crawl_function = jikan_crawl_anime
        else:
            crawl_function = crawl_anime
    if args.profile:
        subscription_name = os.getenv("SCHEDULE_PROFILE_PUBSUB_SUBSCRIPTION")
        if args.jikan:
            crawl_function = jikan_crawl_profile
        else:
            crawl_function = crawl_profile

    max_messages = args.max_urls

    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_name
    )
    with subscriber:
        while True:
            try:
                response = subscriber.pull(
                    request = {"subscription" : subscription_path, "max_messages" : max_messages},
                    retry = retry.Retry(deadline=300)
                )
            except Exception as e:
                logging.warning(e)
                # sleep between 60 and 200 seconds
                sleep_time = 20 * random.randint(3, 10)
                time.sleep(sleep_time)
                continue
            
            received_messages = response.received_messages
            if len(received_messages) == 0:
                logging.warning("No urls pulled from PubSub")
                # sleep between 60 and 200 seconds
                sleep_time = 20 * random.randint(3, 10)
                time.sleep(sleep_time)
                continue
            
            urls = []
            ack_ids = []
            for received_message in received_messages:
                urls.append(received_message.message.data.decode('utf-8'))
                ack_ids.append(received_message.ack_id)
            
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

            logging.info(f"Fetched {len(urls)} urls")
            crawl_function(urls)