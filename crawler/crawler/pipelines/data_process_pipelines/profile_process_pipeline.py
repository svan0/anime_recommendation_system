from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.profile_item import ProfileItem

class ProfileProcessPipeline:
    """
        Drop profile items that do not statisy integrity constraints
    """    
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):

        if not isinstance(item, ProfileItem):
            return item
        
        if 'url' not in item:
            raise DropItem("ProfileItem dropped because 'url' is null")
        
        not_null_fields = [
            'crawl_date', 'uid', 'last_online_date',
            'num_watching', 'num_completed', 'num_on_hold',
            'num_dropped', 'num_plan_to_watch', 'num_days', 'mean_score'
        ]
        for field in not_null_fields:
            if field not in item:
                raise DropItem(f"ProfileItem {item['url']} dropped because '{field}' is null")
        
        logging.debug(f"ProfileItem {item['url']} processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item


