from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.recommendation_item import RecommendationItem

class RecommendationProcessPipeline:
    """
        Drop recommendation items that do not statisy integrity constraints
    """
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):

        if not isinstance(item, RecommendationItem):
            return item
        
        if 'num_recs' not in item:
            item['num_recs'] = 1
        
        item['num_recs'] = item['num_recs'] + 1
        
        if 'url' not in item:
            raise DropItem("RecommendationItem dropped because 'url' is null")

        for field in ['crawl_date', 'src_anime', 'dest_anime', 'num_recs']:
            if field not in item:
                raise DropItem(f"RecommendationItem {item['url']} dropped because '{field}' is null")
        
        logging.debug("RecommendationItem processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item

        
        
