import time
import re
from datetime import datetime

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

import scrapy
from scrapy.http import Request
from scrapy.loader import ItemLoader


from crawler.items.data_items.profile_item import ProfileItem
from crawler.items.data_items.favorite_item import FavoriteItem
from crawler.items.data_items.activity_item import ActivityItem
from crawler.items.data_items.watch_status_item import WatchStatusItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem

class ProfileSpider(scrapy.Spider):
    name = 'profile'
    allowed_domains = ['myanimelist.net']
    start_urls = ['https://myanimelist.net/profile/svanO']

    def parse(self, response):
        self.logger.info('Parsing profile url:  %s', response.url)

        url = response.url
        user_id = response.url.split('/')[4]

        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(url)
        time.sleep(1)

        scrapy_selector = scrapy.selector.Selector(text = driver.page_source)
        driver.quit()

        for status_url in scrapy_selector.xpath('//div[@class="stats anime"]/div/ul/li/a/@href').getall():
            yield Request(url = status_url, 
                        callback = self.parse_anime_status
            )
        
        activity_url = scrapy_selector.xpath('//div[@class="user-profile-sns"]/a[contains(@href, "rw&")]/@href').get()
        yield Request(url = activity_url,
                      callback = self.parse_user_activity
        )

        favorites = scrapy_selector.xpath('//ul[@class="favorites-list anime"]/li/div[2]/a/@href').getall()
        for favorite in favorites:
            favorite_loader = ItemLoader(item=FavoriteItem())
            favorite_loader.add_value('user_id', user_id)
            favorite_loader.add_value('anime_id', favorite)
            yield favorite_loader.load_item()

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', favorite)
            anime_schedule_loader.add_value('last_inspect_date', datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            yield anime_schedule_loader.load_item()
        
        profile_loader = ItemLoader(item=ProfileItem(), selector = scrapy_selector)
        profile_loader.add_value('url', url)
        profile_loader.add_value('uid', user_id)
        profile_loader.add_xpath('num_forum_posts', '//a[contains(.//span, "Forum Posts")]/span[2]/text()')
        profile_loader.add_xpath('num_reviews', '//a[contains(.//span, "Reviews")]/span[2]/text()')
        profile_loader.add_xpath('num_recommendations', '//a[contains(.//span, "Recommendations")]/span[2]/text()')
        profile_loader.add_xpath('num_blog_posts', '//a[contains(.//span, "Blog Posts")]/span[2]/text()')

        profile_loader.add_xpath('num_days', '//div[@class="stats anime"]/div/div[contains(.//span, "Days:")]/text()')
        profile_loader.add_xpath('mean_score', '//div[@class="stats anime"]/div/div[contains(.//span, "Mean Score:")]/span[2]/text()')

        yield Request(
            url = f"{url}/clubs",
            callback  = self.parse_clubs,
            meta = {"profile_item" : profile_loader.load_item()}
        )
    
    def parse_clubs(self, response):
        self.logger.info('Parsing profile clubs url:  %s', response.url)

        profile_loader = ItemLoader(item=response.meta['profile_item'], response=response)
        profile_loader.add_xpath('clubs', '//table/tr/td/ol/li/a/@href')
        profile_loader.add_value('crawl_date', datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        
        yield profile_loader.load_item()
    
    def parse_user_activity(self, response):
        self.logger.info('Parsing user activity RSS url:  %s', response.url)

        user_id = response.url.split("=")[-1]
        activities = response.xpath('//item')
        for activity in activities:
            activity_loader = ItemLoader(item=ActivityItem(), selector=activity)
            activity_loader.add_value('user_id', user_id)
            activity_loader.add_xpath('anime_id', './/link/text()')
            activity_loader.add_xpath('activity_type', './/description/text()')
            activity_loader.add_xpath('date', './/pubDate/text()')
            activity_loader.add_value('crawl_date', datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            yield activity_loader.load_item()

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), selector=activity)
            anime_schedule_loader.add_xpath('url', './/link/text()')
            anime_schedule_loader.add_value('last_inspect_date', datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            yield anime_schedule_loader.load_item()
    
    def parse_anime_status(self, response):
        self.logger.info('Parsing anime status url:  %s', response.url)

        status_mapping = {
            "1" : "watching",
            "2" : "completed",
            "3" : "on_hold",
            "4" : "dropped",
            "6" : "plan_to_watch"
        }

        user_id = response.url.split('/')[4].split("?status=")[0]
        anime_status = response.url.split('/')[4].split("?status=")[1]
        anime_status = status_mapping[anime_status]

        url = response.url

        pause_time = 1
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(url)

        time.sleep(pause_time)

        next_scroll_height = driver.execute_script("return document.body.scrollHeight;")

        while True:
            # scroll one screen height each time
            driver.execute_script("window.scrollTo(0, {scroll_height});".format(scroll_height=next_scroll_height))  
            time.sleep(pause_time)
            # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            current_scroll_height = next_scroll_height
            next_scroll_height = driver.execute_script("return document.body.scrollHeight;")  
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if current_scroll_height >= next_scroll_height:
                break

        scrapy_selector = scrapy.selector.Selector(text = driver.page_source)
        driver.quit()

        #Extract animes
        animes = scrapy_selector.xpath('//tbody[@class="list-item"]')
        for anime in animes:
            watch_status_loader = ItemLoader(item=WatchStatusItem(), selector=anime)
            watch_status_loader.add_value('user_id', user_id)
            watch_status_loader.add_xpath('anime_id', './/td[contains(@class, "title")]/a/@href')
            watch_status_loader.add_value('status', anime_status)
            watch_status_loader.add_xpath('score', './/td[contains(@class, "score")]/a/span/text()')
            if anime_status == "completed":
                watch_status_loader.add_xpath('progress', './/td[contains(@class, "progress")]/div/span/text()')
            else:
                watch_status_loader.add_xpath('progress', './/td[contains(@class, "progress")]/div/span/a/text()')
            
            watch_status_loader.add_value('crawl_date', datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            yield watch_status_loader.load_item()

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), selector=anime)
            anime_schedule_loader.add_xpath('url', './/td[contains(@class, "title")]/a/@href')
            anime_schedule_loader.add_value('last_inspect_date', datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            yield anime_schedule_loader.load_item()
        