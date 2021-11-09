from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.favorite_item import FavoriteItem

class FavoriteProcessPipeline:

    def process_item(self, item, spider):
        if not isinstance(item, FavoriteItem):
            return item

        for field in ['crawl_date', 'user_id', 'anime_id']:
            if field not in item:
                raise DropItem(f"FavoriteItem dropped because '{field}' is null")
        
        logging.info("FavoriteItem processed")
        return item
