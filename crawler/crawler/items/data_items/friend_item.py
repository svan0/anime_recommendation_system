import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class FriendItem(scrapy.Item):
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    src_profile = scrapy.Field(
        input_processor = MapCompose(get_user_id),
        output_processor = TakeFirst()
    )
    dest_profile = scrapy.Field(
        input_processor = MapCompose(get_user_id),
        output_processor = TakeFirst()
    )
    friendship_date = scrapy.Field(
        input_processor = MapCompose(lambda x : str(x).split("Friends since ")[1], parse_date),
        output_processor = TakeFirst()
    )