from datetime import datetime

import scrapy
from scrapy.loader import ItemLoader

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem

class TopAnimeSpider(scrapy.Spider):
    name = 'top_anime'
    allowed_domains = ['myanimelist.net']

    def parse(self, response):
        self.logger.info('Parse function called on %s', response.url)
        for link in response.xpath('//tr[@class="ranking-list"]/td[contains(@class, "title")]/a/@href').getall():
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', link)
            yield anime_schedule_loader.load_item()
