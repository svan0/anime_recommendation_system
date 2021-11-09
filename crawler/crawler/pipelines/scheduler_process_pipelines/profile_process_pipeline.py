from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class ProfileSchedulerProcessPipeline:
    
    def process_item(self, item, spider):
        
        if not isinstance(item, ProfileSchedulerItem):
            return item
        
        if 'url' not in item:
            raise DropItem("ProfileSchedulerItem dropped because 'url' is null")

        if 'last_inspect_date' not in item:
            raise DropItem(f"ProfileSchedulerItem {item['url']} dropped because 'last_inspect_date' is null")

        logging.info(f"ProfileSchedulerItem {item['url']} processed")

        return item