from datetime import datetime

from scrapy.exceptions import DropItem
from crawler.items.data_items.favorite_item import FavoriteItem

class FavoriteProcessPipeline:

    def process_item(self, item, spider):
        if not isinstance(item, FavoriteItem):
            return item
        
        item['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        for field in ['crawl_date', 'user_id', 'anime_id']:
            if field not in item:
                raise DropItem(f"FavoriteItem dropped because '{field}' is null")
        
        return item
