import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class ReviewItem(scrapy.Item):
    """
        User/Anime review item
    """
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    url = scrapy.Field(
        input_processor = MapCompose(get_url),
        output_processor = TakeFirst()
    )
    uid = scrapy.Field(
        input_processor = MapCompose(get_review_id),
        output_processor = TakeFirst()
    )
    anime_id = scrapy.Field(
        input_processor = MapCompose(get_anime_id),
        output_processor = TakeFirst()
    )
    user_id = scrapy.Field(
        input_processor = MapCompose(get_user_id),
        output_processor = TakeFirst()
    )
    review_date = scrapy.Field(
        input_processor = MapCompose(parse_date),
        output_processor = TakeFirst()
    )
    num_useful = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    overall_score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    story_score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    animation_score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    sound_score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    character_score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    enjoyment_score = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )