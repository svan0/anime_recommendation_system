import time
from datetime import datetime
import requests

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

import scrapy
from scrapy.http import Request, HtmlResponse
from scrapy.loader import ItemLoader


from crawler.items.data_items.profile_item import ProfileItem
from crawler.items.data_items.favorite_item import FavoriteItem
from crawler.items.data_items.activity_item import ActivityItem
from crawler.items.data_items.watch_status_item import WatchStatusItem
from crawler.items.data_items.friend_item import FriendItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class ProfileSpider(scrapy.Spider):
    name = 'profile'
    allowed_domains = ['myanimelist.net']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crawl_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--disable-dev-shm-usage')   
        chrome_options.add_argument("window-size=1920,1080")

        self.driver = webdriver.Chrome(
            ChromeDriverManager().install(), 
            options = chrome_options
        )
    
    def closed(self, reason):
        self.driver.quit()

    def parse_profile_main_page(self, response, local_file_response = False):
        
        profile_loader = ItemLoader(item=ProfileItem(), response = response)
        
        profile_loader.add_value('url', response.url)
        profile_loader.add_value('uid', response.url)
        
        profile_loader.add_xpath('last_online_date', '//li[contains(./span, "Last Online")]/span[contains(@class, "user-status-data")]/text()')
        profile_loader.add_xpath('num_forum_posts', '//a[contains(./span, "Forum Posts")]/span[2]/text()')
        profile_loader.add_xpath('num_reviews', '//a[contains(./span, "Reviews")]/span[2]/text()')
        profile_loader.add_xpath('num_recommendations', '//a[contains(./span, "Recommendations")]/span[2]/text()')
        profile_loader.add_xpath('num_blog_posts', '//a[contains(./span, "Blog Posts")]/span[2]/text()')

        profile_loader.add_xpath('num_days', '//div[@class="stats anime"]/div/div[contains(.//span, "Days:")]/text()')
        profile_loader.add_xpath('mean_score', '//div[@class="stats anime"]/div/div[contains(.//span, "Mean Score:")]/span[2]/text()')

        return profile_loader.load_item()

    def parse_profile_main_page_for_favorites(self, response, local_file_response = False):
        
        favorites = response.xpath('//ul[@class="favorites-list anime"]/li/div[2]/a/@href').getall()
        for favorite in favorites:
            favorite_loader = ItemLoader(item=FavoriteItem())
            favorite_loader.add_value('crawl_date', self.crawl_date)
            favorite_loader.add_value('user_id', response.url)
            favorite_loader.add_value('anime_id', favorite)
            yield favorite_loader.load_item()
    
    def parse_profile_main_page_for_favorites_scheduler(self, response, local_file_response = False):
        
        favorites = response.xpath('//ul[@class="favorites-list anime"]/li/div[2]/a/@href').getall()
        for favorite in favorites:
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', favorite)
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
    
    def parse_friend_page_for_friends(self, response, local_file_response = False):

        src_profile = response.url.split('/friends?offset')[0].split("/profile/")[1]
        friends = response.xpath('//div[@class = "friendBlock"]')
        for friend in friends:
            friend_loader = ItemLoader(item=FriendItem(), selector=friend)
            friend_loader.add_value('crawl_date', self.crawl_date)
            friend_loader.add_value('src_profile', src_profile)
            friend_loader.add_xpath('dest_profile', './div[2]/a/@href')
            friend_loader.add_xpath('friendship_date', './div[4]/text()')
            yield friend_loader.load_item()
    
    def parse_friend_page_for_schedule_profiles(self, response, local_file_response = False):
        friends = response.xpath('//div[@class = "friendBlock"]')
        for friend in friends:
            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem(), selector=friend)
            profile_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            profile_schedule_loader.add_xpath('url', './div[2]/a/@href')
            yield profile_schedule_loader.load_item()
    
    def parse_clubs_page_for_clubs(self, response, local_file_response = False):
        
        if local_file_response == False:
            profile_loader = ItemLoader(item=ProfileItem(), response=response)
            profile_loader.add_xpath('clubs', '//table/tr/td/ol/li/a/@href')
            return profile_loader.load_item()
        else:
            profile_loader = ItemLoader(item=ProfileItem(), response=response)
            profile_loader.add_xpath('clubs', '//table/tbody/tr/td/ol/li/a/@href')
            return profile_loader.load_item()
    
    def parse_activity_page_for_activity(self, response, local_file_response = False):
        
        activities = response.xpath('//item')
        for activity in activities:
            activity_loader = ItemLoader(item=ActivityItem(), selector=activity)
            activity_loader.add_value('crawl_date', self.crawl_date)
            activity_loader.add_value('user_id', response.url)
            activity_loader.add_xpath('anime_id', './link/text()')
            activity_loader.add_xpath('activity_type', './description/text()')
            activity_loader.add_xpath('date', './pubDate/text()')
            yield activity_loader.load_item()

    def parse_activity_page_for_scheduler(self, response, local_file_response = False):
        
        activities = response.xpath('//item')
        for activity in activities:
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), selector=activity)
            anime_schedule_loader.add_xpath('url', './/link/text()')
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
    
    def parse_status_page_for_anime_status(self, response, local_file_response = False):
        
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

        animes = response.xpath('//table[./tbody/tr/td/a[contains(@href, "/anime/")]]/tbody/tr[1][not(contains(@class, "header"))]')
        for anime in animes:
            watch_status_loader = ItemLoader(item=WatchStatusItem(), selector=anime)
            watch_status_loader.add_value('crawl_date', self.crawl_date)
            watch_status_loader.add_value('user_id', user_id)
            watch_status_loader.add_xpath('anime_id', './td/a[contains(@href, "/anime/")]/@href')
            watch_status_loader.add_value('status', anime_status)
            watch_status_loader.add_xpath('score', './td//span[contains(@class, "score")]/text()')
            if anime_status == "completed":
                watch_status_loader.add_xpath('progress', './td[5]/text()')
                watch_status_loader.add_xpath('progress', './td[contains(@class, "progress")]/div//span/text()')
            else:
                watch_status_loader.add_xpath('progress', './td[5]/span/text()')
                watch_status_loader.add_xpath('progress', './td[contains(@class, "progress")]/div//span/a/text()')
            
            yield watch_status_loader.load_item()
        self.logger.info(f"{response.url} scrapped {len(animes)} from")
    
    def parse_status_page_for_anime_schedule(self, response, local_file_response = False):
        
        animes = response.xpath('//table[./tbody/tr/td/a[contains(@href, "/anime/")]]/tbody/tr[1][not(contains(@class, "header"))]')
        for anime in animes:
            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem(), selector=anime)
            anime_schedule_loader.add_xpath('url', './td/a[contains(@href, "/anime/")]/@href')
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()

    def parse(self, response):
        self.logger.info('Parsing profile url:  %s', response.url)

        self.logger.info(requests.get('http://checkip.dyndns.org/').text)

        self.driver.get(response.url)
        time.sleep(1)

        response = HtmlResponse(
            url=response.url,
            request=Request(url=response.url),
            body=self.driver.page_source,
            encoding = 'utf-8'
        )

        friends_url = f"{response.url}/friends?offset=0"
        yield Request(url = friends_url,
                      callback = self.parse_friends
        )

        for status_url in response.xpath('//div[@class="stats anime"]/div/ul/li/a/@href').getall():
            yield Request(url = status_url, 
                        callback = self.parse_anime_status
            )
        
        activity_url = response.xpath('//div[@class="user-profile-sns"]/a[contains(@href, "rw&")]/@href').get()
        yield Request(url = activity_url,
                      callback = self.parse_user_activity
        )

        for favorite in self.parse_profile_main_page_for_favorites(response):
            yield favorite
        
        for favorite_schedule in self.parse_profile_main_page_for_favorites_scheduler(response):
            yield favorite_schedule
        
        profile_item = self.parse_profile_main_page(response)
        
        yield Request(
            url = f"{response.url}/clubs",
            callback  = self.parse_clubs,
            meta = {"profile_item" : profile_item}
        )
    
    def parse_friends(self, response):
        self.logger.info('Parsing friends url:  %s', response.url)
        base_url = response.url.split("?offset=")[0]
        offset = response.url.split("?offset=")[1]
        offset = int(offset)

        friends_page_empty = True

        for friend in self.parse_friend_page_for_friends(response):
            friends_page_empty = False
            yield friend
        
        for profile_schedule in self.parse_friend_page_for_schedule_profiles(response):
            yield profile_schedule

        if not friends_page_empty:
            next_page = f"{base_url}?offset={(offset + 100)}"
            yield Request(url = next_page, 
                          callback = self.parse_friends
            )

    def parse_clubs(self, response):
        self.logger.info('Parsing profile clubs url:  %s', response.url)

        profile_item = response.meta['profile_item']
        profile_item = ProfileItem({**profile_item, **self.parse_clubs_page_for_clubs(response)})
        profile_item['crawl_date'] = self.crawl_date
        yield profile_item

        profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
        profile_schedule_loader.add_value('url', profile_item['url'])
        profile_schedule_loader.add_value('last_online_date', profile_item['last_online_date'] if 'last_online_date' in profile_item else None)
        profile_schedule_loader.add_value('last_crawl_date', profile_item['crawl_date'])
        profile_schedule_loader.add_value('last_inspect_date', profile_item['crawl_date'])
        yield profile_schedule_loader.load_item()
    
    def parse_user_activity(self, response):
        self.logger.info('Parsing user activity RSS url:  %s', response.url)

        for activity in self.parse_activity_page_for_activity(response):
            yield activity
        
        for activity_anime_schedule in self.parse_activity_page_for_scheduler(response):
            yield activity_anime_schedule
        
    def parse_anime_status(self, response):
        self.logger.info('Parsing anime status url:  %s', response.url)

        pause_time = 10

        self.driver.get(response.url)

        time.sleep(pause_time)

        
        next_scroll_height = self.driver.execute_script("return document.body.scrollHeight;")

        while True:
            # scroll one screen height each time
            self.driver.execute_script("window.scrollTo(0, {scroll_height});".format(scroll_height=next_scroll_height))  
            time.sleep(pause_time)
            # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            current_scroll_height = next_scroll_height
            next_scroll_height = self.driver.execute_script("return document.body.scrollHeight;")  
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if current_scroll_height >= next_scroll_height:
                break

        response = HtmlResponse(
            url=response.url,
            request=Request(url=response.url),
            body=self.driver.page_source,
            encoding = 'utf-8'
        )

        #Extract animes
        for anime_status in self.parse_status_page_for_anime_status(response):
            yield anime_status
        
        for anime_schedule in self.parse_status_page_for_anime_schedule(response):
            yield anime_schedule
        