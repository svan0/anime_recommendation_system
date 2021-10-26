import os
import json
from datetime import datetime

from google.cloud import pubsub

from scrapy.loader import ItemLoader

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.profile_item import ProfileItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from dotenv import load_dotenv

class PubSubPipeline:
    def __init__(self):
        load_dotenv()
        self.publish_client = None
        self.project = os.getenv('PROJECT_ID')
        self.topic = os.getenv('DATA_PUBSUB_TOPIC')

    def open_spider(self, spider):
        self.publish_client = pubsub.PublisherClient()
        self.topic_path = self.publish_client.topic_path(
            self.project, self.topic
        )

    def process_item(self, item, spider):

        if isinstance(item, AnimeSchedulerItem):
            return item
        if isinstance(item, ProfileSchedulerItem):
            return item

        message = dict(item)
        message["message_type"] = item.__class__.__name__
        message = json.dumps(message).encode("utf-8")

        self.publish_client.publish(self.topic_path, message)

        if isinstance(item, AnimeItem):
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', item['url'])
            anime_schedule_loader.add_value('end_date', item['end_date'] if 'end_date' in item else None)
            anime_schedule_loader.add_value('watching_count', item['watching_count'])
            anime_schedule_loader.add_value('last_crawl_date', item['crawl_date'])
            anime_schedule_loader.add_value('last_inspect_date', item['crawl_date'])
            return anime_schedule_loader.load_item()

        elif isinstance(item, ProfileItem):
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('url', item['url'])
            profile_schedule_loader.add_value('last_online_date', item['last_online_date'])
            profile_schedule_loader.add_value('last_crawl_date', item['crawl_date'])
            profile_schedule_loader.add_value('last_inspect_date', item['crawl_date'])
            return profile_schedule_loader.load_item()
        
        else:
            return item


    
