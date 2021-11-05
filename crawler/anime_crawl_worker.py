import argparse
import os
import time
import logging

from dotenv import load_dotenv
import crochet
crochet.setup()

from google.api_core import retry
from google.cloud import pubsub

from scrapy.utils.project import get_project_settings

from utils import GCP_PIPELINES, LOCAL_PIPELINES
from utils import run_anime_crawler

def local_crawl_anime(anime_urls):
    crawler_settings = get_project_settings()
    crawler_settings['ITEM_PIPELINES'] = LOCAL_PIPELINES
    run_anime_crawler(anime_urls, crawler_settings)

def cloud_crawl_anime(anime_urls):
    crawler_settings = get_project_settings()
    crawler_settings['ITEM_PIPELINES'] = GCP_PIPELINES
    run_anime_crawler(anime_urls, crawler_settings)
    


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--local', dest='local', action='store_true')
    parser.add_argument('--max_urls', type=int)
    parser.set_defaults(local = False)
    parser.set_defaults(max_urls = 10)
    args = parser.parse_args()

    if args.local:
        load_dotenv()
        crawl_function = local_crawl_anime
    else:
        crawl_function = cloud_crawl_anime
    
    max_messages = args.max_urls

    project_id = os.getenv("PROJECT_ID")
    subscription_name = os.getenv("SCHEDULE_ANIME_PUBSUB_SUBSCRIPTION")
    
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
                time.sleep(10)
                continue
            
            received_messages = response.received_messages
            if len(received_messages) == 0:
                logging.warning("No urls pulled from PubSub")
                time.sleep(10)
                continue
            
            anime_urls = []
            ack_ids = []
            for received_message in received_messages:
                anime_urls.append(received_message.message.data.decode('utf-8'))
                ack_ids.append(received_message.ack_id)
            
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

            crawl_function(anime_urls)