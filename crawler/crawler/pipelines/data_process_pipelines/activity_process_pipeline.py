import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.activity_item import ActivityItem

class ActivityProcessPipeline:
    """
        Drop activity items that do not statisy integrity constraints
    """
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):
        if not isinstance(item, ActivityItem):
            return item
        
        for field in ['crawl_date', 'user_id', 'anime_id', 'activity_type', 'date']:
            if field not in item:
                raise DropItem(f"ActivityItem dropped because '{field}' is null")

        logging.debug(f"ActivityItem processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item

