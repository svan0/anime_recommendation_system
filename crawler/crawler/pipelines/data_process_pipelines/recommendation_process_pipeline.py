from datetime import datetime

from scrapy.exceptions import DropItem
from crawler.items.data_items.recommendation_item import RecommendationItem

class RecommendationProcessPipeline:

    def process_item(self, item, spider):

        if not isinstance(item, RecommendationItem):
            return item
        
        item['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        if 'num_recs' not in item:
            item['num_recs'] = 1
        
        item['num_recs'] = item['num_recs'] + 1
        
        if 'url' not in item:
            raise DropItem("RecommendationItem dropped because 'url' is null")

        for field in ['crawl_date', 'src_anime', 'dest_anime', 'num_recs']:
            if field not in item:
                raise DropItem(f"RecommendationItem {item['url']} dropped because '{field}' is null")
        
        return item
        
        