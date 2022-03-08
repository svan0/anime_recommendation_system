import os
import json
import datetime
from pathlib import Path
from collections import defaultdict

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.review_item import ReviewItem
from crawler.items.data_items.recommendation_item import RecommendationItem
from crawler.items.data_items.related_anime_item import RelatedAnimeItem
from crawler.items.data_items.profile_item import ProfileItem
from crawler.items.data_items.favorite_item import FavoriteItem
from crawler.items.data_items.activity_item import ActivityItem
from crawler.items.data_items.watch_status_item import WatchStatusItem
from crawler.items.data_items.friend_item import FriendItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from crawler.utils import crawler_stats_timing

class LocalJSONSavePipeline:
    """
        Scrapy Item Pipeline that processes data items (not schedule items)
        by writing each data item to a json file. 
        IMPORTANT: use this only for testing purposes or you will end up
        with millions of small files (it is a pain to delete them)
        AnimeSchedule and ProfileSchedule with defined crawl date are also returned
        after processing Anime and Profile item respectively
    """
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)
    
    def open_spider(self, spider):
        self.folder_path = Path(os.path.realpath(__file__)).parent.parent.parent.absolute()

        os.makedirs(os.path.join(self.folder_path, 'local_output/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/AnimeItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/ReviewItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/RecommendationItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/RelatedAnimeItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/ProfileItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/FavoriteItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/ActivityItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/WatchStatusItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'local_output/FriendItem/'), exist_ok=True)

    @crawler_stats_timing
    def process_item(self, item, spider):
        
        if isinstance(item, AnimeItem):
            uid = item['uid']
        
        elif isinstance(item, ReviewItem):
            uid = item['uid']
        
        elif isinstance(item, RecommendationItem):
            uid = f"{item['src_anime']}_{item['dest_anime']}"
        
        elif isinstance(item, RelatedAnimeItem):
            uid = f"{item['src_anime']}_{item['dest_anime']}"
        
        elif isinstance(item, ProfileItem):
            uid = item['uid']
        
        elif isinstance(item, WatchStatusItem):
            uid = f"{item['user_id']}_{item['anime_id']}"
        
        elif isinstance(item, FavoriteItem):
            uid = f"{item['user_id']}_{item['anime_id']}"
        
        elif isinstance(item, ActivityItem):
            uid = f"{item['user_id']}_{item['anime_id']}_{item['activity_type']}_{item['date']}"
        
        elif isinstance(item, FriendItem):
            uid = f"{item['src_profile']}_{item['dest_profile']}"
        
        else:
            return item
        
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)

        item_class = item.__class__.__name__
        time_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S:%f")

        file_path = os.path.join(self.folder_path, f"local_output/{item_class}/{uid}_{time_now}.json")
        
        with open(file_path, 'w+') as f:
            f.write(json.dumps(dict(item), indent=4, sort_keys=True))
        
        return item

class BatchedLocalJSONSavePipeline:
    """
        Scrapy Item Pipeline that processes data items (not schedule items)
        by batching them and writing them too a JSON file when spider closed (finished crawling)
        AnimeSchedule and ProfileSchedule with defined crawl date are also returned
        after processing Anime and Profile item respectively
    """
    def __init__(self, stats = None):
        self.batched_data = defaultdict(list)
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def open_spider(self, spider):
        self.folder_path = Path(os.path.realpath(__file__)).parent.parent.parent.absolute()

        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/AnimeItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/ReviewItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/RecommendationItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/RelatedAnimeItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/ProfileItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/FavoriteItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/ActivityItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/WatchStatusItem/'), exist_ok=True)
        os.makedirs(os.path.join(self.folder_path, 'batched_local_output/FriendItem/'), exist_ok=True)

    @crawler_stats_timing
    def process_item(self, item, spider):
        if isinstance(item, AnimeSchedulerItem):
            return item
        if isinstance(item, ProfileSchedulerItem):
            return item
        
        self.batched_data[item.__class__.__name__].append(dict(item))
        
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        
        return item
    
    @crawler_stats_timing
    def close_spider(self, spider):
        time_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S:%f")
        for data_type in self.batched_data.keys():
            file_path = os.path.join(self.folder_path, f"batched_local_output/{data_type}/{time_now}.json")
            
            with open(file_path, 'w+') as f:
                f.write(json.dumps(self.batched_data[data_type], indent=4, sort_keys=True))
        