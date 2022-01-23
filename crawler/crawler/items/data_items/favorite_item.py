import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class FavoriteItem(scrapy.Item):
    """
        User/Anime favorite item
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