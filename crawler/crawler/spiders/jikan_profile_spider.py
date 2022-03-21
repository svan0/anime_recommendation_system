from datetime import datetime
import json

import scrapy
from scrapy.http import Request
from scrapy.loader import ItemLoader


from crawler.items.data_items.profile_item import ProfileItem
from crawler.items.data_items.favorite_item import FavoriteItem
from crawler.items.data_items.activity_item import ActivityItem
from crawler.items.data_items.watch_status_item import WatchStatusItem
from crawler.items.data_items.friend_item import FriendItem

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

class JikanProfileSpider(scrapy.Spider):
    """
        Spider that crawls profile data from the Jikan API
    """
    name = 'jikan_profile'

    def __init__(self, *args, **kwargs):
        self.stats = None
        super().__init__(*args, **kwargs)
        self.crawl_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self.anime_list_error_count = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.start_urls = spider.crawler.settings.get('start_urls')
        spider.stats = spider.crawler.stats
        return spider

    def parse_user_statistics(self, response):
        """
            Extract profile item statistics from
            https://api.jikan.moe/v4/users/{username}/statistics
            Forwards to extarcting profile item clubs
        """
        user_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        profile_loader = ItemLoader(item=ProfileItem())
        profile_loader.add_value('num_days', api_result['data']['anime']['days_watched'])
        profile_loader.add_value('mean_score', api_result['data']['anime']['mean_score'])
        profile_loader.add_value('num_watching', api_result['data']['anime']['watching'])
        profile_loader.add_value('num_completed', api_result['data']['anime']['completed'])
        profile_loader.add_value('num_on_hold', api_result['data']['anime']['on_hold'])
        profile_loader.add_value('num_dropped', api_result['data']['anime']['dropped'])
        profile_loader.add_value('num_plan_to_watch', api_result['data']['anime']['plan_to_watch'])

        profile_item = response.meta['profile_item']
        profile_item = ProfileItem({**profile_item, **profile_loader.load_item()})
        yield Request(url = f'https://api.jikan.moe/v4/users/{user_id}/clubs', 
                      callback = self.parse_user_clubs,
                      meta = {"profile_item" : profile_item}
        )

    def parse_user_clubs(self, response):
        """
            Extract profile item clubs
            https://api.jikan.moe/v4/users/{username}/clubs
        """
        user_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        profile_loader = ItemLoader(item=ProfileItem())
        for club in api_result['data']:
            profile_loader.add_value('clubs', club['mal_id'])

        profile_item = response.meta['profile_item']
        profile_item = ProfileItem({**profile_item, **profile_loader.load_item()})
        profile_item['crawl_date'] = self.crawl_date
        yield profile_item
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{profile_item.__class__.__name__}', count = 1)

    def parse_user_favorites(self, response):
        """
            Extract profile favorite anime and anime schedule info from
            https://api.jikan.moe/v4/users/{username}/favorites
        """
        user_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        for favorite in api_result['data']['anime']:
            favorite_loader = ItemLoader(item=FavoriteItem())
            favorite_loader.add_value('crawl_date', self.crawl_date)
            favorite_loader.add_value('user_id', user_id)
            favorite_loader.add_value('anime_id', str(favorite['mal_id']))
            yield favorite_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{favorite_loader.load_item().__class__.__name__}', count = 1)

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_schedule_loader.add_value('url', favorite['url'])
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)

    def parse_user_friends(self, response):
        """
            Extract profile friends and profile schedule item from
            https://api.jikan.moe/v4/users/{username}/friends
        """
        user_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' not in api_result:
            return
        for friend in api_result['data']:
            friend_loader = ItemLoader(item=FriendItem())
            friend_loader.add_value('crawl_date', self.crawl_date)
            friend_loader.add_value('src_profile', user_id)
            friend_loader.add_value('dest_profile', friend['user']['username'])
            friend_loader.add_value('friendship_date', friend['friends_since'])
            yield friend_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{friend_loader.load_item().__class__.__name__}', count = 1)

            profile_schedule_loader = ItemLoader(item=ProfileSchedulerItem())
            profile_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            profile_schedule_loader.add_value('url', friend['user']['url'])
            yield profile_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{profile_schedule_loader.load_item().__class__.__name__}', count = 1)

    def parse_user_history(self, response):
        """
            Extract last animes that profile interacted with from
            'https://api.jikan.moe/v4/users/{username}/history/anime'
            Yields activity items and anime schedule items
        """
        user_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))
        
        if 'data' not in api_result:
            return

        for activity in api_result['data']:
            activity_loader = ItemLoader(item=ActivityItem())
            activity_loader.add_value('crawl_date', self.crawl_date)
            activity_loader.add_value('user_id', user_id)
            activity_loader.add_value('anime_id', activity['entry']['mal_id'])
            activity_loader.add_value('activity_type', 'Watching')
            activity_loader.add_value('date', activity['date'])
            yield activity_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{activity_loader.load_item().__class__.__name__}', count = 1)

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_id = activity['entry']['mal_id']
            anime_schedule_loader.add_value('url', f'https://myanimelist.net/anime/{anime_id}')
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)
    
    def parse_user_animelist(self, response):
        """
            Extracts profile watch list from
            'https://api.jikan.moe/v4/users/{username}/animelist?page={page_num}'
            Yields WatchStatus items and anime schedule items
            Forwards to next animelist page
        """

        status_mapping = {
            "1" : "watching",
            "2" : "completed",
            "3" : "on_hold",
            "4" : "dropped",
            "6" : "plan_to_watch"
        }

        user_id = response.url.split('/')[5]
        api_result = json.loads(response.body.decode('utf-8'))

        current_page_num = int(response.url.split('page=')[1])
        next_page_url = f"{response.url.split('page=')[0]}page={current_page_num + 1}"

        if 'data' not in api_result:
            self.anime_list_error_count += 1
            if self.anime_list_error_count == 5:
                return
            
            yield Request(url = next_page_url, callback = self.parse_user_animelist)
            return 

        self.anime_list_error_count = 0

        for anime in api_result['data']:
            watch_status_loader = ItemLoader(item=WatchStatusItem())
            watch_status_loader.add_value('crawl_date', self.crawl_date)
            watch_status_loader.add_value('user_id', user_id)
            watch_status_loader.add_value('anime_id', str(anime['anime']['mal_id']))
            watch_status_loader.add_value('status', status_mapping[str(anime['watching_status'])])
            watch_status_loader.add_value('score', str(anime['score']) if anime['score'] > 0 else None)
            watch_status_loader.add_value('progress', str(anime['episodes_watched']))
            yield watch_status_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{watch_status_loader.load_item().__class__.__name__}', count = 1)

            anime_schedule_loader = ItemLoader(item=AnimeSchedulerItem())
            anime_id = anime['anime']['mal_id']
            anime_schedule_loader.add_value('url', f'https://myanimelist.net/anime/{anime_id}')
            anime_schedule_loader.add_value('last_inspect_date', self.crawl_date)
            yield anime_schedule_loader.load_item()
            if self.stats:
                self.stats.inc_value(f'{self.__class__.__name__}_processed_{anime_schedule_loader.load_item().__class__.__name__}', count = 1)


        if len(api_result['data']) == 300:
            yield Request(url = next_page_url, callback = self.parse_user_animelist)
        
        self.logger.debug(f"{response.url} scrapped {len(api_result['data'])} from")
    
    def parse(self, response):
        """
            Entry point of the crawler.
            Extracts user info from
            https://api.jikan.moe/v4/users/{username}
            Forwards to user stats, user favorite, user friends, user hisory and user anime lists pages
        """
        self.logger.debug('Parsing profile url using Jikan:  %s', response.url)

        user_id = response.url.split('/users/')[1]
        api_result = json.loads(response.body.decode('utf-8'))
        if 'data' in api_result:
            profile_loader = ItemLoader(item=ProfileItem())
            profile_loader.add_value('url', api_result['data']['url'])
            profile_loader.add_value('uid', api_result['data']['username'])
            profile_loader.add_value('last_online_date', api_result['data']['last_online'])
            profile_item = profile_loader.load_item()
            yield Request(
                url = f'https://api.jikan.moe/v4/users/{user_id}/statistics',
                callback  = self.parse_user_statistics,
                meta = {"profile_item" : profile_item}
            )

        user_favorites_url = f'https://api.jikan.moe/v4/users/{user_id}/favorites'
        yield Request(url = user_favorites_url, callback = self.parse_user_favorites)

        user_friends_url = f'https://api.jikan.moe/v4/users/{user_id}/friends'
        yield Request(url = user_friends_url, callback = self.parse_user_friends)

        user_history_url = f'https://api.jikan.moe/v4/users/{user_id}/history/anime'
        yield Request(url = user_history_url, callback = self.parse_user_history)
        
        user_animelist_url = f'https://api.jikan.moe/v4/users/{user_id}/animelist?page=1'
        yield Request(url = user_animelist_url, callback = self.parse_user_animelist)
