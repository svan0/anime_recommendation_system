import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class ProfileItem(scrapy.Item):
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
    num_forum_posts = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_reviews = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_recommendations = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    num_blog_posts = scrapy.Field(
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