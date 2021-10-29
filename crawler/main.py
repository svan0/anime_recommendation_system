from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

from crawler.spiders.anime_spider import AnimeSpider
from crawler.spiders.profile_spider import ProfileSpider
from crawler.spiders.top_anime_spider import TopAnimeSpider
from crawler.spiders.recent_profile_spider import RecentProfileSpider

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

if __name__ == '__main__':
    local_crawl_profile('https://myanimelist.net/profile/svanO')
