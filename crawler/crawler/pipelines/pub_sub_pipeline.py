import os
import json
import logging

from google.cloud import pubsub

from scrapy.loader import ItemLoader

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.profile_item import ProfileItem
from crawler.items.data_items.favorite_item import FavoriteItem
from crawler.items.data_items.review_item import ReviewItem
from crawler.items.data_items.watch_status_item import WatchStatusItem
from crawler.items.data_items.activity_item import ActivityItem
from crawler.items.data_items.recommendation_item import RecommendationItem
from crawler.items.data_items.related_anime_item import RelatedAnimeItem
from crawler.items.data_items.friend_item import FriendItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class PubSubPipeline:
    def __init__(self):
        self.publish_client = None
        self.project = os.getenv('PROJECT_ID')
        self.ingestion_topic = os.getenv('DATA_INGESTION_PUBSUB_TOPIC')

    def open_spider(self, spider):
        self.publish_client = pubsub.PublisherClient()

    def process_item(self, item, spider):

        if isinstance(item, AnimeSchedulerItem):
            return item
        if isinstance(item, ProfileSchedulerItem):
            return item

        message = {}
        message['data'] = dict(item)
        
        topic_path = self.publish_client.topic_path(
            self.project, self.ingestion_topic
        )

        if isinstance(item, AnimeItem):
            message['bq_table'] = "anime_item"
        
        elif isinstance(item, ProfileItem):
            message['bq_table'] = "profile_item"
        
        elif isinstance(item, FavoriteItem):
            message['bq_table'] = "favorite_item"
        
        elif isinstance(item, ReviewItem):
            message['bq_table'] = "review_item"
        
        elif isinstance(item, WatchStatusItem):
            message['bq_table'] = "watch_status_item"

        elif isinstance(item, ActivityItem):
            message['bq_table'] = "activity_item"
        
        elif isinstance(item, RecommendationItem):
            message['bq_table'] = "recommendation_item"
        
        elif isinstance(item, RelatedAnimeItem):
            message['bq_table'] = "related_anime_item"
        
        elif isinstance(item, FriendItem):
            message['bq_table'] = "friends_item"

        else:
            return item
        
        message = json.dumps(message).encode("utf-8")
        self.publish_client.publish(topic_path, message)
        logging.info(f"Published to PubSub {self.ingestion_topic}")

        if isinstance(item, AnimeItem):
            logging.info(f"anime {item['url']} prepare anime schedule")
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', item['url'])
            anime_schedule_loader.add_value('end_date', item['end_date'] if 'end_date' in item else None)
            anime_schedule_loader.add_value('watching_count', item['watching_count'])
            anime_schedule_loader.add_value('last_crawl_date', item['crawl_date'])
            anime_schedule_loader.add_value('last_inspect_date', item['crawl_date'])
            return anime_schedule_loader.load_item()

        elif isinstance(item, ProfileItem):
            logging.info(f"profile {item['url']} prepare profile schedule")
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('url', item['url'])
            profile_schedule_loader.add_value('last_online_date', item['last_online_date'])
            profile_schedule_loader.add_value('last_crawl_date', item['crawl_date'])
            profile_schedule_loader.add_value('last_inspect_date', item['crawl_date'])
            return profile_schedule_loader.load_item()
        
        else:
            return item


    
