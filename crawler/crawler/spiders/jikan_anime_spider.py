from datetime import datetime
import json

import scrapy
from scrapy.http import Request
from scrapy.loader import ItemLoader

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.review_item import ReviewItem
from crawler.items.data_items.recommendation_item import RecommendationItem
from crawler.items.data_items.related_anime_item import RelatedAnimeItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class JikanAnimeSpider(scrapy.Spider):
    """
        Spider that crawls anime data from the Jikan API.
    """
    name = 'jikan_anime'

    def __init__(self, *args, **kwargs):
        self.stats = None
        super().__init__(*args, **kwargs)
        self.crawl_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.start_urls = spider.crawler.settings.get('start_urls')
        spider.stats = spider.crawler.stats
        return spider
    
    def parse_anime_reviews(self, response):
        """
            Extract anime review items and profile schedule items from
            https://api.jikan.moe/v4/anime/{id}/reviews?page={page_num}
        """
        anime_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        current_page_num = int(response.url.split('page=')[1])
        next_page_url = f"{response.url.split('page=')[0]}page={current_page_num + 1}"
        
        if 'data' not in api_result:
            self.reviews_error_count += 1
            if self.reviews_error_count == 5:
                return
            
            yield Request(url = next_page_url, callback = self.parse_anime_reviews)
            return 
        self.reviews_error_count = 0
        for review in api_result['data']:
            review_loader = ItemLoader(item=ReviewItem())
            review_loader.add_value('crawl_date', self.crawl_date)
            review_loader.add_value('url', review['url'])
            review_loader.add_value('uid', review['mal_id'])
            review_loader.add_value('anime_id', anime_id)
            review_loader.add_value('user_id', review['user']['username'])
            review_loader.add_value('review_date', review['date'])
            review_loader.add_value('num_useful', review['votes'])
            review_loader.add_value('overall_score', review['scores']['overall'])
            review_loader.add_value('story_score', review['scores']['story'])
            review_loader.add_value('animation_score', review['scores']['animation'])
            review_loader.add_value('sound_score', review['scores']['sound'])
            review_loader.add_value('character_score', review['scores']['character'])
            review_loader.add_value('enjoyment_score', review['scores']['enjoyment'])
            yield review_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{review_loader.load_item().__class__.__name__}', count = 1)

            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            profile_schedule_loader.add_value('url', review['user']['url'])
            yield profile_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{profile_schedule_loader.load_item().__class__.__name__}', count = 1)
        
        if len(api_result['data']) == 20:
            yield Request(url = next_page_url, callback = self.parse_anime_reviews)
        
    def parse_anime_related_anime(self, response):
        """
            Extarct anime related anime items and anime schedule items from
            https://api.jikan.moe/v4/anime/{id}/relations
        """
        anime_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        for relation_type in api_result['data']: 
            for related_anime in relation_type['entry']:
                if related_anime['type'] == 'manga':
                    continue
                related_anime_loader = ItemLoader(item=RelatedAnimeItem())
                related_anime_loader.add_value('crawl_date', self.crawl_date)
                related_anime_loader.add_value('src_anime', anime_id)
                related_anime_loader.add_value('dest_anime', str(related_anime['mal_id']))
                related_anime_loader.add_value('relation_type', relation_type['relation'])
                yield related_anime_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{related_anime_loader.load_item().__class__.__name__}', count = 1)

                anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
                anime_schedule_loader.add_value('url', related_anime['url'])
                anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
                yield anime_schedule_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)


    def parse_anime_recommendations(self, response):
        """
            Extract anime recommendation items and anime schedule items from
            https://api.jikan.moe/v4/anime/{id}/recommendations
        """
        anime_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        for recommended_anime in api_result['data']:
            rec_loader = ItemLoader(item=RecommendationItem())
            rec_loader.add_value('crawl_date', self.crawl_date)
            rec_loader.add_value('url', recommended_anime['url'])
            rec_loader.add_value('src_anime', anime_id)
            rec_loader.add_value('dest_anime', str(recommended_anime['entry']['mal_id']))
            rec_loader.add_value('num_recs', recommended_anime['votes'])
            yield rec_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{rec_loader.load_item().__class__.__name__}', count = 1)

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', recommended_anime['entry']['url'])
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)

    def parse_anime_statistics(self, response):
        """
            Extracts anime item statistics from 
            https://api.jikan.moe/v4/anime/{id}/statistics
            Combines with extarcted info from 'parse' method
        """
        anime_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        anime_loader = ItemLoader(item=AnimeItem())
        anime_loader.add_value('watching_count', api_result['data']['watching'])
        anime_loader.add_value('completed_count', api_result['data']['completed'])
        anime_loader.add_value('on_hold_count', api_result['data']['on_hold'])
        anime_loader.add_value('dropped_count', api_result['data']['dropped'])
        anime_loader.add_value('plan_to_watch_count', api_result['data']['plan_to_watch'])
        anime_loader.add_value('total_count', api_result['data']['total'])

        for score in api_result['data']['scores']:
            anime_loader.add_value(f"score_{str(score['score']).zfill(2)}_count", score['votes'])
        
        anime_main_item = response.meta['anime_item']
        anime_item = AnimeItem({**dict(anime_main_item), **dict(anime_loader.load_item())})
        
        yield Request(url = f'https://api.jikan.moe/v4/anime/{anime_id}/pictures', 
                      callback = self.parse_anime_pics,
                      meta = {"anime_item" : anime_item}
        )
    
    def parse_anime_pics(self, response):
        """
            Extracts anime item pics from 
            https://api.jikan.moe/v4/anime/{id}/pictures
            Combines with extracted info from 'parse' and 'parse_anime_statistics' method
        """
        anime_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        anime_loader = ItemLoader(item=AnimeItem())
        for pic in api_result['data']:
            anime_loader.add_value('main_pic', pic['jpg']['image_url'])
            anime_loader.add_value('pics', pic['jpg']['image_url'])
        
        anime_main_item = response.meta['anime_item']
        anime_item = AnimeItem({**dict(anime_main_item), **dict(anime_loader.load_item())})
        yield anime_item

        
    def parse(self, response):
        """
            Entry point for crawler
            Exract anime item information from
            https://api.jikan.moe/v4/anime/{id}
            Forwards to crawling recommendations, related anime, reviews and anime stats
        """

        anime_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        
        anime_loader = ItemLoader(item=AnimeItem())
        anime_loader.add_value('crawl_date', self.crawl_date)
        anime_loader.add_value('uid', api_result['data']['mal_id'])
        anime_loader.add_value('url', api_result['data']['url'])
        anime_loader.add_value('title', api_result['data']['title'])
        anime_loader.add_value('synopsis', api_result['data']['synopsis'])
        anime_loader.add_value('type', api_result['data']['type'])
        anime_loader.add_value('source_type', api_result['data']['source'])
        anime_loader.add_value('num_episodes', api_result['data']['episodes'])
        anime_loader.add_value('status', api_result['data']['status'])
        anime_loader.add_value('start_date', api_result['data']['aired']['from'])
        anime_loader.add_value('end_date', api_result['data']['aired']['to'])
        if api_result['data']['season'] is not None and api_result['data']['year'] is not None:
            anime_loader.add_value('season', f"{api_result['data']['season']} {api_result['data']['year']}")
        
        for studio in api_result['data']['studios']:
            anime_loader.add_value('studios', studio['name'])
        
        for genre in api_result['data']['genres']:
            anime_loader.add_value('genres', genre['name'])
        for genre in api_result['data']['explicit_genres']:
            anime_loader.add_value('genres', genre['name'])
        for genre in api_result['data']['themes']:
            anime_loader.add_value('genres', genre['name'])
        for genre in api_result['data']['demographics']:
            anime_loader.add_value('genres', genre['name'])

        anime_loader.add_value('score', api_result['data']['score'])
        anime_loader.add_value('score_count', api_result['data']['scored_by'])
        anime_loader.add_value('score_rank', api_result['data']['rank'])
        anime_loader.add_value('popularity_rank', api_result['data']['popularity'])
        anime_loader.add_value('members_count', api_result['data']['members'])
        anime_loader.add_value('favorites_count', api_result['data']['favorites'])
        
        anime_item = anime_loader.load_item()
        yield Request(url = f'https://api.jikan.moe/v4/anime/{anime_id}/statistics', 
                      callback = self.parse_anime_statistics,
                      meta = {"anime_item" : anime_item}
        )

        anime_recommendations_url = f'https://api.jikan.moe/v4/anime/{anime_id}/recommendations'
        yield Request(url = anime_recommendations_url, callback = self.parse_anime_recommendations)

        anime_related_url = f'https://api.jikan.moe/v4/anime/{anime_id}/relations'
        yield Request(url = anime_related_url, callback = self.parse_anime_related_anime)

        anime_review_url = f'https://api.jikan.moe/v4/anime/{anime_id}/reviews?page=1'
        yield Request(url = anime_review_url, callback = self.parse_anime_reviews)

