from datetime import datetime
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.review_item import ReviewItem

class ReviewProcessPipeline:
    """
        Drop Review items that do not statisy integrity constraints
    """
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):

        if not isinstance(item, ReviewItem):
            return item
        
        if 'url' not in item:
            raise DropItem("ReviewItem dropped because url is null")
        
        fields_not_null = [
            'crawl_date', 'uid', 'anime_id', 'user_id', 'review_date', 
            'num_useful', 'overall_score', 'story_score', 'animation_score',
            'sound_score', 'character_score', 'enjoyment_score'
        ]
        
        for field in fields_not_null:
            if field not in item:
                raise DropItem(f"ReviewItem {item['url']} dropped because {field} is null")
        
        logging.debug(f"ReviewItem {item['url']} processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item
