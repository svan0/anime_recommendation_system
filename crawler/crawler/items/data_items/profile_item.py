import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class ProfileItem(scrapy.Item):
    """
        User profile item that specifies profile information
    """
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    uid = scrapy.Field(
        input_processor = MapCompose(get_user_id),
        output_processor = TakeFirst()
    )
    url = scrapy.Field(
        input_processor = MapCompose(get_url),
        output_processor = TakeFirst()
    )
    last_online_date = scrapy.Field(
        input_processor = MapCompose(get_last_online_date),
        output_processor = TakeFirst()
    )
    num_watching = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_completed = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_on_hold = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_dropped = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_plan_to_watch = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_days = scrapy.Field(
        input_processor = MapCompose(transform_to_float),
        output_processor = TakeFirst()
    )
    mean_score = scrapy.Field(
        input_processor = MapCompose(transform_to_float),
        output_processor = TakeFirst()
    )
    clubs = scrapy.Field(
        input_processor = MapCompose(get_club_id)
    )