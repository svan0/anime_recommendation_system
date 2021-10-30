from datetime import datetime

from scrapy.exceptions import DropItem
from crawler.items.data_items.related_anime_item import RelatedAnimeItem

class RelatedAnimeProcessPipeline:
    
    def process_item(self, item, spider):

        if not isinstance(item, RelatedAnimeItem):
            return item
        
        item['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        for field in ['crawl_date', 'src_anime', 'dest_anime']:
            if field not in item:
                raise DropItem(f"RelatedAnimeItem dropped because {field} is null")
        
        return item