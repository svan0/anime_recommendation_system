from datetime import datetime

import scrapy
from scrapy.loader import ItemLoader

from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class RecentProfileSpider(scrapy.Spider):
    name = 'recent_profile'
    allowed_domains = ['myanimelist.net']
    start_urls = [
        'https://myanimelist.net/users.php'
    ]

    def parse(self, response):
        self.logger.info('Parse function called on %s', response.url)
        for link in response.xpath('//tr/td/div[1]/a[contains(@href, "/profile/")]/@href').getall():
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('url', link)
            yield profile_schedule_loader.load_item()
