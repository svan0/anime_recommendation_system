"""
    Helper functions for running crawling jobs
"""
import random

import crochet
crochet.setup()

from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from crawler.spiders.anime_spider import AnimeSpider
from crawler.spiders.profile_spider import ProfileSpider
from crawler.spiders.jikan_profile_spider import JikanProfileSpider
from crawler.spiders.jikan_anime_spider import JikanAnimeSpider
from crawler.spiders.top_anime_spider import TopAnimeSpider
from crawler.spiders.recent_profile_spider import RecentProfileSpider

def get_item_pipelines(crawled_output = None, scheduler_db_type = None):
    """
        Helper function that returns the scrapy item pipeline
        based on where we want to export the crawled data 
        and which database type we use for scheduling crawling
    """
    item_piplines = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.friend_process_pipeline.FriendProcessPipeline': 100,    
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 100
    }

    if crawled_output == 'json':
        item_piplines['crawler.pipelines.local_json_pipeline.BatchedLocalJSONSavePipeline'] = 300
    if crawled_output == 'pubsub':
        item_piplines['crawler.pipelines.pub_sub_pipeline.PubSubPipeline'] = 300
    if crawled_output == 'bigquery':
        item_piplines['crawler.pipelines.big_query_pipeline.BigQueryPipeline'] = 300
    if crawled_output == 'all':
        item_piplines['crawler.pipelines.local_json_pipeline.BatchedLocalJSONSavePipeline'] = 300
        item_piplines['crawler.pipelines.pub_sub_pipeline.PubSubPipeline'] = 300
        item_piplines['crawler.pipelines.big_query_pipeline.BigQueryPipeline'] = 300

    if scheduler_db_type == 'postgres':
        item_piplines['crawler.pipelines.scheduler_db_pipeline.SchedulerPostgresPipeline'] = 500
    if scheduler_db_type == 'sqlite':
        item_piplines['crawler.pipelines.scheduler_db_pipeline.SchedulerSQLitePipeline'] = 500
    
    return item_piplines


def clean_url(url, type='anime', source='myanimelist'):
    if type == 'anime' and source == 'myanimelist':
        try:
            url = url.split('/anime/')[1].split('/')[0]
            return f"https://myanimelist.net/anime/{url}"
        except:
            return f"https://myanimelist.net/anime/{url}"
    
    if type == 'anime' and source == 'jikan':
        try:
            url = url.split('/anime/')[1].split('/')[0]
            return f"https://api.jikan.moe/v4/anime/{url}"
        except:
            return f"https://api.jikan.moe/v4/anime/{url}"
    
    if type == 'profile' and source == 'myanimelist':
        try:
            url = url.split('/profile/')[1]
            return f"https://myanimelist.net/profile/{url}"
        except:
            try:
                url = url.split('/users/')[1]
                return f"https://myanimelist.net/profile/{url}"
            except:
                return f"https://myanimelist.net/profile/{url}"
    
    if type == 'profile' and source == 'jikan':
        try:
            url = url.split('/users/')[1]
            return f"https://api.jikan.moe/v4/users/{url}"
        except:
            try:
                url = url.split('/profile/')[1]
                return f"https://api.jikan.moe/v4/users/{url}"
            except:
                return f"https://api.jikan.moe/v4/users/{url}"
    

@crochet.wait_for(timeout = 3600)
def run_top_anime_crawler(num_pages, crawler_settings):
    """
        Run TopAnimeSpider to inspect Random animes and insert them in schedule database 
    """
    start_rank = random.randint(0, 15000)

    urls = [
            f'https://myanimelist.net/topanime.php?limit={x}'
            for x in range(start_rank, start_rank + 50 * num_pages, 50)
    ]

    crawler_settings['start_urls'] = urls
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(TopAnimeSpider)
    return deferred

@crochet.wait_for(timeout = 3600)
def run_recent_profile_crawler(crawler_settings):
    """
        Run RecentProfileSpider to inspect Random profiles and insert them in schedule database 
    """
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(RecentProfileSpider)
    return deferred

@crochet.wait_for(timeout = 18000)
def run_anime_crawler(anime_urls, crawler_settings):
    """
        Run AnimeSpider to crawl animes from myanimelist.net
    """
    anime_urls = list(map(lambda x : clean_url(x, type='anime', source='myanimelist'), anime_urls))
    crawler_settings['start_urls'] = anime_urls
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(AnimeSpider)
    return deferred

@crochet.wait_for(timeout = 18000)
def run_jikan_anime_crawler(anime_urls, crawler_settings):
    """
        Run JikanAnimeSpider to crawl animes from the Jikan API
    """
    anime_urls = list(map(lambda x : clean_url(x, type='anime', source='jikan'), anime_urls))
    crawler_settings['start_urls'] = anime_urls
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(JikanAnimeSpider)
    return deferred

@crochet.wait_for(timeout = 36000)
def run_profile_crawler(profile_urls, crawler_settings):
    """
        Run ProfileSpider to crawl profiles from myanimelist.net
    """
    profile_urls = list(map(lambda x : clean_url(x, type='profile', source='myanimelist'), profile_urls))
    crawler_settings['start_urls'] = profile_urls
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(ProfileSpider)
    return deferred

@crochet.wait_for(timeout = 36000)
def run_jikan_profile_crawler(profile_urls, crawler_settings):
    """
        Run JikanProfileSpider to crawl profiles from the Jikan API
    """
    profile_urls = list(map(lambda x : clean_url(x, type='profile', source='jikan'), profile_urls))
    crawler_settings['start_urls'] = profile_urls
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(JikanProfileSpider)
    return deferred