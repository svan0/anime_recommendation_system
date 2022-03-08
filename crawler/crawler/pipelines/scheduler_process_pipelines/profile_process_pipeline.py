from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class ProfileSchedulerProcessPipeline:
    """
        Drop profile schedule items that do not statisy integrity constraints
    """  
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):
        
        if not isinstance(item, ProfileSchedulerItem):
            return item
        
        if 'url' not in item:
            raise DropItem("ProfileSchedulerItem dropped because 'url' is null")

        if 'last_inspect_date' not in item:
            raise DropItem(f"ProfileSchedulerItem {item['url']} dropped because 'last_inspect_date' is null")

        logging.debug(f"ProfileSchedulerItem {item['url']} processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item

