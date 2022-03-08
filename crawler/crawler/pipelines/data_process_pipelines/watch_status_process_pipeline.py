from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.watch_status_item import WatchStatusItem

class WatchStatusProcessPipeline:
    """
        Drop watch status items that do not statisy integrity constraints
    """    
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):
        
        if not isinstance(item, WatchStatusItem):
            return item
        
        fields_not_null = [
            'crawl_date', 'user_id', 'anime_id', 'status'
        ]
        for field in fields_not_null:
            if field not in item:
                raise DropItem(f"WatchStatusItem dropped because '{field}' is null")
        
        logging.debug("WatchStatusItem processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item
