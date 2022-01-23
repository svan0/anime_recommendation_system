import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst

from crawler.items.data_items.utils.utils import *

class WatchStatusItem(scrapy.Item):
    """
        User/Anime status item
    """
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    user_id = scrapy.Field(
        input_processor = MapCompose(get_user_id),
        output_processor = TakeFirst()
    )
    anime_id = scrapy.Field(
        input_processor = MapCompose(get_anime_id),
        output_processor = TakeFirst()
    )
    status = scrapy.Field(
        output_processor = TakeFirst()
    )
    score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    progress = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )