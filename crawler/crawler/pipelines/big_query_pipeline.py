from itertools import count
import os
import logging
from collections import defaultdict

from google.cloud import bigquery

from scrapy.loader import ItemLoader

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.profile_item import ProfileItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from crawler.utils import crawler_stats_timing

class BigQueryPipeline:
    """
        Scrapy Item Pipeline that processes data items (not schedule items)
        by batching them and appending them to theirr respective BigQuery table
        when spider closed (finished crawling)
        AnimeSchedule and ProfileSchedule with defined crawl date are also returned
        after processing Anime and Profile item respectively
    """
    def __init__(self, stats = None):
        self.publish_client = None
        self.project = os.getenv('PROJECT_ID')
        self.client = bigquery.Client(project=self.project)
        self.item_type_to_bq_table_name = {
            'AnimeItem' : 'anime_item',
            'ProfileItem' : 'profile_item',
            'FavoriteItem' : 'favorite_item',
            'ReviewItem' : 'review_item',
            'WatchStatusItem' : 'watch_status_item',
            'ActivityItem' : 'activity_item',
            'RecommendationItem' : 'recommendation_item',
            'RelatedAnimeItem' : 'related_anime_item',
            'FriendItem' : 'friends_item'
        }
        self.job_config = {}
        for data_type in self.item_type_to_bq_table_name.keys():
            self.job_config[data_type] = bigquery.LoadJobConfig(
                write_disposition='WRITE_APPEND', 
                source_format = 'JSON',
                schema = self.client.get_table(f"{self.project}.landing_area.{self.item_type_to_bq_table_name[data_type]}").schema
            )
        self.items_to_ingest = defaultdict(list)
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)
    
    @crawler_stats_timing
    def close_spider(self, spider):
        for data_type in self.items_to_ingest.keys():
            table_name = f"{self.project}.landing_area.{self.item_type_to_bq_table_name[data_type]}"
            job_config = self.job_config[data_type]
            self.client.load_table_from_json(self.items_to_ingest[data_type], table_name, job_config=job_config)
        self.client.close()

    @crawler_stats_timing
    def process_item(self, item, spider):

        if isinstance(item, AnimeSchedulerItem):
            return item
        if isinstance(item, ProfileSchedulerItem):
            return item

        self.items_to_ingest[item.__class__.__name__].append(dict(item))
        
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)

        if isinstance(item, AnimeItem):
            logging.debug(f"anime {item['url']} prepare anime schedule")
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', item['url'])
            anime_schedule_loader.add_value('last_crawl_date', item['crawl_date'])
            return anime_schedule_loader.load_item()

        elif isinstance(item, ProfileItem):
            logging.debug(f"profile {item['url']} prepare profile schedule")
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('url', item['url'])
            profile_schedule_loader.add_value('last_crawl_date', item['crawl_date'])
            return profile_schedule_loader.load_item()
        
        else:
            return item
