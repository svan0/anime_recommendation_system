import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class RelatedAnimeItem(scrapy.Item):
    """
        Anime/Anime related item
    """
    crawl_date = scrapy.Field(
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
    relation_type = scrapy.Field(
        output_processor = TakeFirst()
    )