import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class AnimeSchedulerItem(scrapy.Item):
    url = scrapy.Field(
        input_processor = MapCompose(get_url),
        output_processor = TakeFirst()
    )

    status = scrapy.Field(
        output_processor = TakeFirst()
    )
    num_watch = scrapy.Field(
        output_processor = TakeFirst()
    )
    num_completed = scrapy.Field(
        output_processor = TakeFirst()
    )
    num_dropped = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    last_crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    last_inspect_date = scrapy.Field(
        output_processor = TakeFirst()
    )