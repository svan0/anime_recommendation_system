import os
import json
from datetime import datetime

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

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class PubSubPipeline:
    def __init__(self):
        self.publish_client = None
        self.project = os.getenv('PROJECT_ID')
        self.anime_topic = os.getenv('ANIME_ITEM_PUBSUB_TOPIC')
        self.profile_topic = os.getenv('PROFILE_ITEM_PUBSUB_TOPIC')
        self.favorite_topic = os.getenv('FAVORITE_ITEM_PUBSUB_TOPIC')
        self.review_topic = os.getenv('REVIEW_ITEM_PUBSUB_TOPIC')
        self.watch_status_topic = os.getenv('WATCH_STATUS_ITEM_PUBSUB_TOPIC')
        self.activity_topic = os.getenv('ACTIVITY_ITEM_PUBSUB_TOPIC')
        self.recommendation_topic = os.getenv('RECOMMENDATION_ITEM_PUBSUB_TOPIC')
        self.related_anime_topic = os.getenv('RELATED_ANIME_ITEM_PUBSUB_TOPIC')

    def open_spider(self, spider):
        self.publish_client = pubsub.PublisherClient()

    def process_item(self, item, spider):

        if isinstance(item, AnimeSchedulerItem):
            return item
        if isinstance(item, ProfileSchedulerItem):
            return item

        message = dict(item)
        message = json.dumps(message).encode("utf-8")

        if isinstance(item, AnimeItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.anime_topic
            )
        if isinstance(item, ProfileItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.profile_topic
            )
        if isinstance(item, FavoriteItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.favorite_topic
            )
        if isinstance(item, ReviewItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.review_topic
            )
        if isinstance(item, WatchStatusItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.watch_status_topic
            )
        if isinstance(item, ActivityItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.activity_topic
            )
        if isinstance(item, RecommendationItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.recommendation_topic
            )
        if isinstance(item, RelatedAnimeItem):
            topic_path = self.publish_client.topic_path(
                self.project, self.related_anime_topic
            )

        self.publish_client.publish(topic_path, message)

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


    
