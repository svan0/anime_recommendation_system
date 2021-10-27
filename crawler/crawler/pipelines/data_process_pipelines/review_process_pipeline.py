from datetime import datetime

from scrapy.exceptions import DropItem
from crawler.items.data_items.review_item import ReviewItem

class ReviewProcessPipeline:

    def process_item(self, item, spider):

        if not isinstance(item, ReviewItem):
            return item
        
        item['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
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
        
        return item