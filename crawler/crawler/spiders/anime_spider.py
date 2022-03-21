from datetime import datetime

import scrapy
from scrapy.http import Request
from scrapy.loader import ItemLoader

from crawler.items.data_items.anime_item import AnimeItem
from crawler.items.data_items.review_item import ReviewItem
from crawler.items.data_items.recommendation_item import RecommendationItem
from crawler.items.data_items.related_anime_item import RelatedAnimeItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class AnimeSpider(scrapy.Spider):
    """
        Spider that crawls anime data from myanimelist.net
    """
    name = 'anime'
    allowed_domains = ['myanimelist.net']
    
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
    
    def parse_anime_main_page_for_info(self, response, local_file_response = False):
        """
            Extract anime information from https://myanimelist.net/anime/{anime_id}
        """
        anime_loader = ItemLoader(item=AnimeItem(), response=response)
        anime_loader.add_value('uid', response.url)
        anime_loader.add_xpath('url', '//li/a[contains(./text(), "Details")]/@href')
        anime_loader.add_xpath('title', '//h1[@class="title-name h1_bold_none"]/strong/text()')
        anime_loader.add_xpath('main_pic', '//img[@itemprop="image"]/@data-src')
        anime_loader.add_xpath('synopsis', '//p[@itemprop="description"]/text()')
        anime_loader.add_xpath('type', '//div[@class="spaceit_pad" and contains(./span/text(), "Type:")]/a/text()')
        anime_loader.add_xpath('source_type', '//div[@class="spaceit_pad" and contains(./span/text(), "Source:")]/text()')
        anime_loader.add_xpath('num_episodes', '//div[@class="spaceit_pad" and contains(./span/text(), "Episodes:")]/text()')
        anime_loader.add_xpath('status', '//div[@class="spaceit_pad" and contains(./span/text(), "Status:")]/text()')
        anime_loader.add_xpath('start_date', '//div[@class="spaceit_pad" and contains(./span/text(), "Aired:")]/text()')
        anime_loader.add_xpath('end_date', '//div[@class="spaceit_pad" and contains(./span/text(), "Aired:")]/text()')
        anime_loader.add_xpath('season', '//div[@class="spaceit_pad" and contains(./span/text(), "Premiered:")]/a/text()')
        anime_loader.add_xpath('studios', '//div[@class="spaceit_pad" and contains(./span/text(), "Studios:")]/a/text()')
        anime_loader.add_xpath('genres', '//span[@itemprop="genre"]/text()')
        anime_loader.add_xpath('score', '//span[@itemprop="ratingValue"]/text()')
        anime_loader.add_xpath('score_count', '//span[@itemprop="ratingCount"]/text()')
        anime_loader.add_xpath('score_rank', '//span[@class="numbers ranked"]/strong/text()')
        anime_loader.add_xpath('popularity_rank', '//span[@class="numbers popularity"]/strong/text()')
        anime_loader.add_xpath('members_count', '//span[@class="numbers members"]/strong/text()')
        anime_loader.add_xpath('favorites_count', '//div[@class="spaceit_pad" and contains(./span/text(), "Favorites:")]/text()')
        return anime_loader.load_item()

    def parse_anime_main_page_for_related_anime(self, response, local_file_response = False):
        """
            Extract anime related anime information from https://myanimelist.net/anime/{anime_id}
        """
        if local_file_response == False:
            for related_anime_relation in response.xpath('//table[@class="anime_detail_related_anime"]/tr'):
                relation_type = related_anime_relation.xpath('./td[1]/text()').get()[:-1]
                for related_anime in related_anime_relation.xpath('./td/a[contains(@href, "/anime/")]/@href').getall():
                    related_anime_loader = ItemLoader(item=RelatedAnimeItem(), response=response)
                    related_anime_loader.add_value('crawl_date', self.crawl_date)
                    related_anime_loader.add_value('src_anime', response.url)
                    related_anime_loader.add_value('dest_anime', related_anime)
                    related_anime_loader.add_value('relation_type', relation_type)
                    yield related_anime_loader.load_item()
                    if self.stats:
                        self.stats.inc_value(f'{self.__class__.__name__}_processed_{related_anime_loader.load_item().__class__.__name__}', count = 1)
        else:
            for related_anime_relation in response.xpath('//table[@class="anime_detail_related_anime"]/tbody/tr'):
                relation_type = related_anime_relation.xpath('./td[1]/text()').get()[:-1]
                for related_anime in related_anime_relation.xpath('./td/a[contains(@href, "/anime/")]/@href').getall():
                    related_anime_loader = ItemLoader(item=RelatedAnimeItem(), response=response)
                    related_anime_loader.add_value('crawl_date', self.crawl_date)
                    related_anime_loader.add_value('src_anime', response.url)
                    related_anime_loader.add_value('dest_anime', related_anime)
                    related_anime_loader.add_value('relation_type', relation_type)
                    yield related_anime_loader.load_item()
                    if self.stats:
                        self.stats.inc_value(f'{self.__class__.__name__}_processed_{related_anime_loader.load_item().__class__.__name__}', count = 1)

    def parse_anime_main_page_for_schedule_anime(self, response, local_file_response = False):
        """
            Extract anime related anime schedule data from https://myanimelist.net/anime/{anime_id}
        """        
        if local_file_response == False:
            for related_anime in response.xpath('//table[@class="anime_detail_related_anime"]/tr/td/a[contains(@href, "/anime/")]/@href').getall():
                anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), response=response)
                anime_schedule_loader.add_value('url', related_anime)
                anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
                yield anime_schedule_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)
        
        else:
            for related_anime in response.xpath('//table[@class="anime_detail_related_anime"]/tbody/tr/td/a[contains(@href, "/anime/")]/@href').getall():
                anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), response=response)
                anime_schedule_loader.add_value('url', related_anime)
                anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
                yield anime_schedule_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)
        
    def parse_review_page_for_reviews(self, response, local_file_response = False):
        """
            Extract anime review information from https://myanimelist.net/anime/{anime_id}/reviews?p={page_num}
        """
        if local_file_response == False:
            for review in response.xpath('//div[@class="borderDark"]'):
                review_loader = ItemLoader(item=ReviewItem(), selector=review)
                review_loader.add_value('crawl_date', self.crawl_date)
                review_loader.add_xpath('url', './/a[@class="lightLink"]/@href')
                review_loader.add_xpath('uid', './/a[@class="lightLink"]/@href')
                review_loader.add_value('anime_id', response.url)
                review_loader.add_xpath('user_id', './/a[contains(@href, "profile")]/@href')
                review_loader.add_xpath('review_date', './/div[@class="spaceit"]/div[@class="mb8"]/div[1]/text()')
                review_loader.add_xpath('num_useful', './/div[@class="lightLink spaceit"]/strong/span/text()')
                review_loader.add_xpath('overall_score', './/table/tr[contains(string(./td), "Overall")]/td[2]/strong/text()')
                review_loader.add_xpath('story_score', './/table/tr[contains(string(./td), "Story")]/td[2]/text()')
                review_loader.add_xpath('animation_score', './/table/tr[contains(string(./td), "Animation")]/td[2]/text()')
                review_loader.add_xpath('sound_score', './/table/tr[contains(string(./td), "Sound")]/td[2]/text()')
                review_loader.add_xpath('character_score', './/table/tr[contains(string(./td), "Character")]/td[2]/text()')
                review_loader.add_xpath('enjoyment_score', './/table/tr[contains(string(./td), "Enjoyment")]/td[2]/text()')
                yield review_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{review_loader.load_item().__class__.__name__}', count = 1)
        else:
            for review in response.xpath('//div[@class="borderDark"]'):
                review_loader = ItemLoader(item=ReviewItem(), selector=review)
                review_loader.add_value('crawl_date', self.crawl_date)
                review_loader.add_xpath('url', './/a[@class="lightLink"]/@href')
                review_loader.add_xpath('uid', './/a[@class="lightLink"]/@href')
                review_loader.add_value('anime_id', response.url)
                review_loader.add_xpath('user_id', './/a[contains(@href, "profile")]/@href')
                review_loader.add_xpath('review_date', './/div[@class="spaceit"]/div[@class="mb8"]/div[1]/text()')
                review_loader.add_xpath('num_useful', './/div[@class="lightLink spaceit"]/strong/span/text()')
                review_loader.add_xpath('overall_score', './/table/tbody/tr[contains(string(./td), "Overall")]/td[2]/strong/text()')
                review_loader.add_xpath('story_score', './/table/tbody/tr[contains(string(./td), "Story")]/td[2]/text()')
                review_loader.add_xpath('animation_score', './/table/tbody/tr[contains(string(./td), "Animation")]/td[2]/text()')
                review_loader.add_xpath('sound_score', './/table/tbody/tr[contains(string(./td), "Sound")]/td[2]/text()')
                review_loader.add_xpath('character_score', './/table/tbody/tr[contains(string(./td), "Character")]/td[2]/text()')
                review_loader.add_xpath('enjoyment_score', './/table/tbody/tr[contains(string(./td), "Enjoyment")]/td[2]/text()')
                yield review_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{review_loader.load_item().__class__.__name__}', count = 1)
    
    def parse_review_page_for_schedule_profiles(self, response, local_file_response = False):
        """
            Extract profile schedule from https://myanimelist.net/anime/{anime_id}/reviews?p={page_num}
            (Users that reviewed the anime)
        """
        for review in response.xpath('//div[@class="borderDark"]'):
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem(), selector=review)
            profile_schedule_loader.add_xpath('url', './/a[contains(@href, "profile")]/@href')
            profile_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield profile_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{profile_schedule_loader.load_item().__class__.__name__}', count = 1)
    
    def parse_recommendation_page_for_recommendations(self, response, local_file_response = False):
        """
            Extract anime recommendation information from https://myanimelist.net/anime/{anime_id}/userrecs
        """
        if local_file_response == False:
            current_anime = response.url.split('/')[4]
            recommendations = response.xpath('//div[@class="borderClass"]/table/tr/td[2]')
            for recommendation in recommendations:
                rec_loader = ItemLoader(item=RecommendationItem(), selector=recommendation)
                rec_loader.add_value('crawl_date', self.crawl_date)
                rec_loader.add_xpath('url', './/div/span/a[@title="Permalink"]/@href')
                rec_loader.add_value('src_anime', current_anime)
                rec_loader.add_xpath('dest_anime', './/div[@style="margin-bottom: 2px;"]/a[contains(@href, "/anime/")]/@href')
                rec_loader.add_xpath('num_recs', './/div[@class="spaceit"]/a[contains(@href, "javascript")]/strong/text()')
                yield rec_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{rec_loader.load_item().__class__.__name__}', count = 1)
        else:
            current_anime = response.url.split('/')[4]
            recommendations = response.xpath('//div[@class="borderClass"]/table/tbody/tr/td[2]')
            for recommendation in recommendations:
                rec_loader = ItemLoader(item=RecommendationItem(), selector=recommendation)
                rec_loader.add_value('crawl_date', self.crawl_date)
                rec_loader.add_xpath('url', './/div/span/a[@title="Permalink"]/@href')
                rec_loader.add_value('src_anime', current_anime)
                rec_loader.add_xpath('dest_anime', './/div[@style="margin-bottom: 2px;"]/a[contains(@href, "/anime/")]/@href')
                rec_loader.add_xpath('num_recs', './/div[@class="spaceit"]/a[contains(@href, "javascript")]/strong/text()')
                yield rec_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{rec_loader.load_item().__class__.__name__}', count = 1)
    
    def parse_recommendation_page_for_schedule_anime(self, response, local_file_response = False):
        """
            Extract anime recommendation anime schedule from https://myanimelist.net/anime/{anime_id}/userrecs
        """
        if local_file_response == False:
            recommendations = response.xpath('//div[@class="borderClass"]/table/tr/td[2]')
            for recommendation in recommendations:
                anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), selector=recommendation)
                anime_schedule_loader.add_xpath('url', './/div[@style="margin-bottom: 2px;"]/a[contains(@href, "/anime/")]/@href')
                anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
                yield anime_schedule_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)
        else:
            recommendations = response.xpath('//div[@class="borderClass"]/table/tbody/tr/td[2]')
            for recommendation in recommendations:
                anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), selector=recommendation)
                anime_schedule_loader.add_xpath('url', './/div[@style="margin-bottom: 2px;"]/a[contains(@href, "/anime/")]/@href')
                anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
                yield anime_schedule_loader.load_item()
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)

    def parse_stats_page_for_stats(self, response, local_file_response = False):
        """
            Extract anime information from https://myanimelist.net/anime/{anime_id}/stats
        """
        if local_file_response == False:
            anime_loader = ItemLoader(item=AnimeItem(), response=response)
            
            anime_loader.add_xpath('watching_count', '//div[contains(./span/text(), "Watching:")]/text()')
            anime_loader.add_xpath('completed_count', '//div[contains(./span/text(), "Completed:")]/text()')
            anime_loader.add_xpath('on_hold_count', '//div[contains(./span/text(), "On-Hold:")]/text()')
            anime_loader.add_xpath('dropped_count', '//div[contains(./span/text(), "Dropped:")]/text()')
            anime_loader.add_xpath('plan_to_watch_count', '//div[contains(./span/text(), "Plan to Watch:")]/text()')
            anime_loader.add_xpath('total_count', '//div[contains(./span/text(), "Total:")]/text()')

            anime_loader.add_xpath("score_10_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "10")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_09_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "9")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_08_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "8")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_07_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "7")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_06_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "6")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_05_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "5")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_04_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "4")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_03_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "3")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_02_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "2")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_01_count", '//table[@class="score-stats"]/tr[contains(./td[contains(@class, "score-label")], "1") and not(contains(./td[contains(@class, "score-label")], "10"))]/td/div/span/small/text()')
        
        else:
            anime_loader = ItemLoader(item=AnimeItem(), response=response)
            
            anime_loader.add_xpath('watching_count', '//div[contains(./span/text(), "Watching:")]/text()')
            anime_loader.add_xpath('completed_count', '//div[contains(./span/text(), "Completed:")]/text()')
            anime_loader.add_xpath('on_hold_count', '//div[contains(./span/text(), "On-Hold:")]/text()')
            anime_loader.add_xpath('dropped_count', '//div[contains(./span/text(), "Dropped:")]/text()')
            anime_loader.add_xpath('plan_to_watch_count', '//div[contains(./span/text(), "Plan to Watch:")]/text()')
            anime_loader.add_xpath('total_count', '//div[contains(./span/text(), "Total:")]/text()')

            anime_loader.add_xpath("score_10_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "10")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_09_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "9")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_08_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "8")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_07_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "7")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_06_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "6")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_05_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "5")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_04_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "4")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_03_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "3")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_02_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "2")]/td/div/span/small/text()')
            anime_loader.add_xpath("score_01_count", '//table[@class="score-stats"]/tbody/tr[contains(./td[contains(@class, "score-label")], "1") and not(contains(./td[contains(@class, "score-label")], "10"))]/td/div/span/small/text()')
        
        return anime_loader.load_item()

    def parse_clubs_page_for_clubs(self, response, local_file_response = False):
        """
            Extract anime information from https://myanimelist.net/anime/{anime_id}/clubs
        """
        anime_loader = ItemLoader(AnimeItem(), response=response)
        anime_loader.add_xpath('clubs', '//div[@class="borderClass"]/a/@href')
        return anime_loader.load_item()

    def parse_pics_page_for_pics(self, response, local_file_response = False):
        """
            Extract anime information from https://myanimelist.net/anime/{anime_id}/pics
        """
        anime_loader = ItemLoader(AnimeItem(), response=response)
        anime_loader.add_xpath('pics', '//div[@class="picSurround"]/a/img/@data-src')
        return anime_loader.load_item()

    def parse(self, response):
        """
            Entry point of the spider.
            Starts with parsing https://myanimelist.net/anime/{anime_id}
            Issues calls to parse reviews, recommendation and anime stats page.
        """
        self.logger.debug('Parsing anime url:  %s', response.url)
        
        anime_item = self.parse_anime_main_page_for_info(response)

        for x in self.parse_anime_main_page_for_related_anime(response):
            yield x
        
        for x in self.parse_anime_main_page_for_schedule_anime(response):
            yield x

        long_url = response.xpath('//li/a[contains(./text(), "Details")]/@href').get()
        
        yield Request(url = f"{long_url}/reviews?p=1", 
                      callback = self.parse_list_reviews,
        )
        
        yield Request(url = f"{long_url}/userrecs", 
                      callback = self.parse_recommendations
        ) 

        yield Request(url = f"{long_url}/stats", 
                      callback = self.parse_stats,
                      meta = {"anime_item" : anime_item}
        )       
        
    def parse_list_reviews(self, response):
        """
            Iterate over review pages and extract reviews from each page
        """
        self.logger.debug('Parsing review url:  %s', response.url)
        p = response.url.split("p=")[1]

        review_page_empty = True
        
        for review in self.parse_review_page_for_reviews(response):
            review_page_empty = False
            yield review
        
        for profile_schedule in self.parse_review_page_for_schedule_profiles(response):
            yield profile_schedule

        next_page = response.xpath('//div[@class="ml4"]/a/@href').getall()
        if (not review_page_empty) and (next_page is not None) and (len(next_page) > 0) and (p == '1' or len(next_page) > 1):
            next_page = next_page[0] if p == '1' else next_page[1]
            yield Request(url = next_page, 
                          callback = self.parse_list_reviews
            )

    def parse_recommendations(self, response):
        """
            Parse anime recommendation page
        """
        self.logger.debug('Parsing recommendations url:  %s', response.url)
        for recommended_anime in self.parse_recommendation_page_for_recommendations(response):
            yield recommended_anime
        
        for schedule_anime in self.parse_recommendation_page_for_schedule_anime(response):
            yield schedule_anime
        
    def parse_stats(self, response):
        """
            Parse stats page. 
            Add extracted info to anime item.
            Forward to clubs page
        """
        self.logger.debug('Parsing stats url:  %s', response.url)
        
        anime_main_item = response.meta['anime_item']
        anime_stats_item = self.parse_stats_page_for_stats(response)

        anime_item = AnimeItem({**dict(anime_main_item), **dict(anime_stats_item)})

        anime_url = anime_main_item['url']
        
        yield Request(url = f"{anime_url}/clubs", 
                      callback = self.parse_clubs,
                      meta = {"anime_item" : anime_item}
        )

    def parse_clubs(self, response):
        """
            Parse clubs page. 
            Add extracted info to anime item.
            Forward to pics page
        """
        self.logger.debug('Parsing clubs url:  %s', response.url)

        anime_main_item = response.meta['anime_item']
        anime_clubs_item = self.parse_clubs_page_for_clubs(response)

        anime_item = AnimeItem({**dict(anime_main_item), **dict(anime_clubs_item)})

        anime_url = anime_main_item['url']
        
        yield Request(url = f"{anime_url}/pics", 
                      callback = self.parse_pics,
                      meta = {"anime_item" : anime_item}
        )

    def parse_pics(self, response):
        """
            Parse pics page.
            Add extracted info to anime item.
        """
        self.logger.debug('Parsing images url:  %s', response.url)
        
        anime_main_item = response.meta['anime_item']
        anime_pics_item = self.parse_pics_page_for_pics(response)

        anime_item = AnimeItem({**dict(anime_main_item), **dict(anime_pics_item)})
        anime_item['crawl_date'] = self.crawl_date

        yield anime_item
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_item.__class__.__name__}', count = 1)