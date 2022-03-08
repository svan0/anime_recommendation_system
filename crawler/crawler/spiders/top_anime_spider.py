from datetime import datetime

import scrapy
from scrapy.loader import ItemLoader

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem

class TopAnimeSpider(scrapy.Spider):
    """
        Spider that crawls myanimelist.net top anime page fro animes to add too scheduler DB
    """
    name = 'top_anime'
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

    def parse(self, response):
        """
            Extracts anime schedule items from https://myanimelist.net/topanime.php?limit={offset}
        """
        self.logger.debug('Parse function called on %s', response.url)
        for link in response.xpath('//tr[@class="ranking-list"]/td[contains(@class, "title")]/a/@href').getall():
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', link)
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)
