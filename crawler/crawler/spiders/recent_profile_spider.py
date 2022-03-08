from datetime import datetime

import scrapy
from scrapy.loader import ItemLoader

from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class RecentProfileSpider(scrapy.Spider):
    """
        Spider that extract recent profiles to add to scheduler DB
    """
    name = 'recent_profile'
    allowed_domains = ['myanimelist.net']
    start_urls = [
        'https://myanimelist.net/users.php'
    ]

    def __init__(self, *args, **kwargs):
        self.stats = None
        super().__init__(*args, **kwargs)
        self.crawl_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.stats = spider.crawler.stats
        return spider

    def parse(self, response):
        """
            Extract recent profiles schedule items from 'https://myanimelist.net/users.php'
        """
        self.logger.debug('Parse function called on %s', response.url)
        for link in response.xpath('//tr/td/div[1]/a[contains(@href, "/profile/")]/@href').getall():
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('url', link)
            profile_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield profile_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{profile_schedule_loader.load_item().__class__.__name__}', count = 1)
