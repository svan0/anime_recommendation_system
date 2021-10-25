import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class ProfileSchedulerItem(scrapy.Item):
    url = scrapy.Field(
        input_processor = MapCompose(get_url),
        output_processor = TakeFirst()
    )    
    last_crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    last_inspect_date = scrapy.Field(
        output_processor = TakeFirst()
    )