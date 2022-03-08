# Scrapy settings for crawler project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

BOT_NAME = 'crawler'

SPIDER_MODULES = ['crawler.spiders']
NEWSPIDER_MODULE = 'crawler.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'crawler (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 3200

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 1
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
CONCURRENT_REQUESTS_PER_IP = 32

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

HTTPERROR_ALLOWED_CODES = [404]

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'crawler.middlewares.AnimeCrawlSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
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
    'crawler.pipelines.local_json_pipeline.BatchedLocalJSONSavePipeline': 100
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
LOG_LEVEL = 'INFO'

if os.path.exists(os.path.join(CURRENT_DIR, '..', 'proxy_list.txt')):
    ROTATING_PROXY_LIST_PATH = os.path.join(CURRENT_DIR, '..', 'proxy_list.txt')
