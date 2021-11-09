from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem

class AnimeSchedulerProcessPipeline:
    
    def process_item(self, item, spider):
        
        if not isinstance(item, AnimeSchedulerItem):
            return item
        
        if 'url' not in item:
            raise DropItem("AnimeSchedulerItem dropped because 'url' is null")

        if 'last_inspect_date' not in item:
            raise DropItem(f"AnimeSchedulerItem {item['url']} dropped because 'last_inspect_date' is null")

        logging.info(f"AnimeSchedulerItem {item['url']} processed")
        
        return item