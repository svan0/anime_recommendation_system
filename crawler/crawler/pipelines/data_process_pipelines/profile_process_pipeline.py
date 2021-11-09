from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.profile_item import ProfileItem

class ProfileProcessPipeline:
    
    def process_item(self, item, spider):

        if not isinstance(item, ProfileItem):
            return item
        
        if 'url' not in item:
            raise DropItem("ProfileItem dropped because 'url' is null")
        
        not_null_fields = [
            'crawl_date', 'uid', 'last_online_date',
            'num_forum_posts', 'num_reviews', 'num_recommendations',
            'num_blog_posts', 'num_days', 'mean_score'
        ]
        for field in not_null_fields:
            if field not in item:
                raise DropItem(f"ProfileItem {item['url']} dropped because '{field}' is null")
        
        logging.info(f"ProfileItem {item['url']} processed")
        return item


