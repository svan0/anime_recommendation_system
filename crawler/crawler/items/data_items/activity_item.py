import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst

from crawler.items.data_items.utils.utils import *

class ActivityItem(scrapy.Item):
    """
        User/Anime activity item
    """
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    user_id = scrapy.Field(
        input_processor = MapCompose(none_text, get_user_id),
        output_processor = TakeFirst()
    )
    anime_id = scrapy.Field(
        input_processor = MapCompose(none_text, get_anime_id),
        output_processor = TakeFirst()
    )
    activity_type = scrapy.Field(
        input_processor = MapCompose(none_text, get_activity_type),
        output_processor = TakeFirst()
    )
    date = scrapy.Field(
        input_processor = MapCompose(none_text, parse_date),
        output_processor = TakeFirst()
    )
