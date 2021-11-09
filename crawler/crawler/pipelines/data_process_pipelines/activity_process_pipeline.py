from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.activity_item import ActivityItem

class ActivityProcessPipeline:

    def process_item(self, item, spider):
        if not isinstance(item, ActivityItem):
            return item
        
        for field in ['crawl_date', 'user_id', 'anime_id', 'activity_type', 'date']:
            if field not in item:
                raise DropItem(f"ActivityItem dropped because '{field}' is null")

        logging.info(f"ActivityItem processed")
        return item