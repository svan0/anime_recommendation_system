import os
import json
from datetime import datetime

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.review_item import ReviewItem
from crawler.items.data_items.recommendation_item import RecommendationItem
from crawler.items.data_items.related_anime_item import RelatedAnimeItem
from crawler.items.data_items.profile_item import ProfileItem
from crawler.items.data_items.favorite_item import FavoriteItem
from crawler.items.data_items.activity_item import ActivityItem
from crawler.items.data_items.watch_status_item import WatchStatusItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from crawler.items.data_items.utils.utils import *

class LocalJSONSavePipeline:

    def open_spider(self, spider):
        os.makedirs('local_output/', exist_ok=True)
        os.makedirs('local_output/AnimeItem/', exist_ok=True)
        os.makedirs('local_output/ReviewItem/', exist_ok=True)
        os.makedirs('local_output/RecommendationItem/', exist_ok=True)
        os.makedirs('local_output/RelatedAnimeItem/', exist_ok=True)
        os.makedirs('local_output/ProfileItem/', exist_ok=True)
        os.makedirs('local_output/FavoriteItem/', exist_ok=True)
        os.makedirs('local_output/ActivityItem/', exist_ok=True)
        os.makedirs('local_output/WatchStatusItem/', exist_ok=True)
        os.makedirs('local_output/AnimeSchedulerItem/', exist_ok=True)
        os.makedirs('local_output/ProfileSchedulerItem/', exist_ok=True)


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
        
        elif isinstance(item, AnimeSchedulerItem):
            uid = get_anime_id(item['url'])
        
        elif isinstance(item, ProfileSchedulerItem):
            uid = get_user_id(item["url"])
        
        else:
            return item
        
        item_class = item.__class__.__name__
        time_now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S:%f")

        file_path = f"local_output/{item_class}/{uid}_{time_now}.json"
        with open(file_path, 'w+') as f:
            f.write(json.dumps(dict(item), indent=4, sort_keys=True))
        
        return item

        