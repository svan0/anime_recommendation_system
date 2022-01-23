import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst, Compose

from crawler.items.data_items.utils.utils import *

class AnimeItem(scrapy.Item):
    """
        Anime item that specifies anime information
    """
    crawl_date = scrapy.Field(
        output_processor = TakeFirst()
    )
    
    uid = scrapy.Field(
        input_processor = MapCompose(get_anime_id),
        output_processor = TakeFirst()
    )
    url = scrapy.Field(
        input_processor = MapCompose(get_url),
        output_processor = TakeFirst()
    )
    title = scrapy.Field(
        input_processor = MapCompose(clean_text),
        output_processor = Join(separator = ' ')
    )
    synopsis = scrapy.Field(
        input_processor = MapCompose(no_synopsis, clean_text),
        output_processor = Join(separator = ' ')
    )
    main_pic = scrapy.Field(
        output_processor = TakeFirst()
    )
    type = scrapy.Field(
        output_processor = TakeFirst()
    )
    source_type = scrapy.Field(
        input_processor = MapCompose(none_text, clean_text),
        output_processor = Join(separator = '')
    )
    num_episodes = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    status = scrapy.Field(
        input_processor = MapCompose(none_text, clean_text),
        output_processor = Join(separator = '')
    )
    start_date = scrapy.Field(
        input_processor = MapCompose(clean_text),
        output_processor = Compose(
            Join(separator = ''),
            MapCompose(extract_start_date),
            TakeFirst()
        )
    )
    end_date = scrapy.Field(
        input_processor = MapCompose(clean_text),
        output_processor = Compose(
            Join(separator = ''),
            MapCompose(extract_end_date),
            TakeFirst()
        )
    )
    season = scrapy.Field(
        output_processor = TakeFirst()
    )
    studios = scrapy.Field(
        input_processor = MapCompose(none_text)
    )
    genres = scrapy.Field(
        input_processor = MapCompose(none_text)
    )
    
    score = scrapy.Field(
        input_processor = MapCompose(transform_to_float),
        output_processor = TakeFirst()
    )
    score_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_rank = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )

    popularity_rank = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    members_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    favorites_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    
    watching_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    completed_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    on_hold_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    dropped_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    plan_to_watch_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    total_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )

    score_10_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_09_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_08_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_07_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_06_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_05_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_04_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_03_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_02_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )
    score_01_count = scrapy.Field(
        input_processor = MapCompose(transform_to_int),
        output_processor = TakeFirst()
    )

    clubs = scrapy.Field(
        input_processor = MapCompose(get_club_id)
    )
    pics = scrapy.Field()
