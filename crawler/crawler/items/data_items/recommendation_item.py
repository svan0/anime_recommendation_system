import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class RecommendationItem(scrapy.Item):
    """
        Anime/Anime recommendation item
    """
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    url = scrapy.Field(
        input_processor = MapCompose(get_url),
        output_processor = TakeFirst()
    )
    src_anime = scrapy.Field(
        input_processor = MapCompose(get_anime_id),
        output_processor = TakeFirst()
    )
    dest_anime = scrapy.Field(
        input_processor = MapCompose(get_anime_id),
        output_processor = TakeFirst()
    )
    num_recs = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )