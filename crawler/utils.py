import random

import crochet
crochet.setup()

from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from crawler.spiders.anime_spider import AnimeSpider
from crawler.spiders.profile_spider import ProfileSpider
from crawler.spiders.top_anime_spider import TopAnimeSpider
from crawler.spiders.recent_profile_spider import RecentProfileSpider

LOCAL_PIPELINES = {
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
    'crawler.pipelines.local_json_pipeline.LocalJSONSavePipeline': 300,
}
GCP_PIPELINES = {
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

@crochet.wait_for(timeout = 300)
def run_top_anime_crawler(num_pages, crawler_settings):
    start_rank = random.randint(0, 15000)

    urls = [
            f'https://myanimelist.net/topanime.php?limit={x}'
            for x in range(start_rank, start_rank + 50 * num_pages, 50)
    ]
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(TopAnimeSpider, start_urls = urls)
    return deferred

@crochet.wait_for(timeout = 60)
def run_recent_profile_crawler(crawler_settings):
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(RecentProfileSpider)
    return deferred

@crochet.wait_for(timeout = 1800)
def run_anime_crawler(anime_urls, crawler_settings):
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(AnimeSpider, start_urls = anime_urls)
    return deferred

@crochet.wait_for(timeout = 3600)
def run_profile_crawler(profile_urls, crawler_settings):
    configure_logging()
    runner = CrawlerRunner(crawler_settings)
    deferred = runner.crawl(ProfileSpider, start_urls = profile_urls)
    return deferred
