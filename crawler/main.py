import base64
import random
import json

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

from crawler.spiders.anime_spider import AnimeSpider
from crawler.spiders.profile_spider import ProfileSpider
from crawler.spiders.top_anime_spider import TopAnimeSpider
from crawler.spiders.recent_profile_spider import RecentProfileSpider

from dotenv import load_dotenv

def local_crawl_top_anime(start_rank, num_pages):
    urls = [
            f'https://myanimelist.net/topanime.php?limit={x}'
            for x in range(start_rank, start_rank + 50 * num_pages, 50)
    ]
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }

    process = CrawlerProcess(settings)
    process.crawl(TopAnimeSpider, start_urls = urls)
    process.start()

def local_crawl_recent_profile():

    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }

    process = CrawlerProcess(settings)
    process.crawl(RecentProfileSpider)
    process.start()

def local_crawl_anime(anime_url):

    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }

    process = CrawlerProcess(settings)
    process.crawl(AnimeSpider, start_urls = [anime_url])
    process.start()

def local_crawl_profile(profile_url):

    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 100,
    }

    process = CrawlerProcess(settings)
    process.crawl(ProfileSpider, start_urls = [profile_url])
    process.start()


def cloud_crawl_top_anime(request):
    start_rank = random.randint(0, 15000)
    num_pages = 20

    urls = [
            f'https://myanimelist.net/topanime.php?limit={x}'
            for x in range(start_rank, start_rank + 50 * num_pages, 50)
    ]
    
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 50
    }

    process = CrawlerProcess(settings)
    process.crawl(TopAnimeSpider, start_urls = urls)
    process.start()

def cloud_crawl_recent_profile(request):
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 10,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 10,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 10,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 50
    }
    process = CrawlerProcess(settings)
    process.crawl(RecentProfileSpider)
    process.start()

def cloud_crawl_anime(event, context):
    anime_url = base64.b64decode(event['data']).decode('utf-8')
    print("ANIME URL : ", anime_url)
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 100,
        'crawler.pipelines.pub_sub_pipeline.PubSubPipeline' : 300,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 500
    }
    process = CrawlerProcess(settings)
    process.crawl(AnimeSpider, start_urls = [anime_url])
    process.start()

def cloud_crawl_profile(event, context):
    profile_url = base64.b64decode(event['data']).decode('utf-8')
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {
        'crawler.pipelines.data_process_pipelines.activity_process_pipeline.ActivityProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.anime_process_pipeline.AnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.favorite_process_pipeline.FavoriteProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.profile_process_pipeline.ProfileProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.recommendation_process_pipeline.RecommendationProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.related_anime_process_pipeline.RelatedAnimeProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.review_process_pipeline.ReviewProcessPipeline': 100,
        'crawler.pipelines.data_process_pipelines.watch_status_process_pipeline.WatchStatusProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.anime_process_pipeline.AnimeSchedulerProcessPipeline': 100,
        'crawler.pipelines.scheduler_process_pipelines.profile_process_pipeline.ProfileSchedulerProcessPipeline': 100,
        'crawler.pipelines.pub_sub_pipeline.PubSubPipeline' : 300,
        'crawler.pipelines.cloud_sql_pipeline.CloudSQLPipeline' : 500
    }
    process = CrawlerProcess(settings)
    process.crawl(ProfileSpider, start_urls = [profile_url])
    process.start()

if __name__ == '__main__':
    load_dotenv()

    anime_url_message = 'https://myanimelist.net/anime/21561/Ryuugajou_Nanana_no_Maizoukin_TV'
    anime_url_message = anime_url_message.encode("utf-8")
    anime_url_message = base64.b64encode(anime_url_message)
    event = {'data' : anime_url_message}
    cloud_crawl_anime(event = event, context = None)

    profile_url_message = 'https://myanimelist.net/profile/svanO'
    profile_url_message = profile_url_message.encode("utf-8")
    profile_url_message = base64.b64encode(profile_url_message)
    event = {'data' : profile_url_message}
    cloud_crawl_profile(event = event, context = None)
