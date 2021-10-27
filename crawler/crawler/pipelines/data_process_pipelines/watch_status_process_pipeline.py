from datetime import datetime

from scrapy.exceptions import DropItem
from crawler.items.data_items.watch_status_item import WatchStatusItem

class WatchStatusProcessPipeline:
    
    def process_item(self, item, spider):
        
        if not isinstance(item, WatchStatusItem):
            return item
        
        item['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        fields_not_null = [
            'url', 'crawl_date', 'watching_count', 'anime_id', 'status'
        ]
        for field in fields_not_null:
            if field not in item:
                raise DropItem(f"WatchStatusItem dropped because '{field}' is null")

        return item