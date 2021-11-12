from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.friend_item import FriendItem

class FriendProcessPipeline:

    def process_item(self, item, spider):
        if not isinstance(item, FriendItem):
            return item
        
        for field in ['crawl_date', 'src_profile', 'dest_profile', 'friendship_date']:
            if field not in item:
                raise DropItem(f"FriendItem dropped because '{field}' is null")

        logging.info(f"FriendItem processed")
        return item