CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user_anime_favorite` AS
WITH user_last_crawl_date AS (
    SELECT user_id, MAX(crawl_date) AS last_crawl_date
    FROM `anime-rec-dev.landing_area.favorite_item`
    GROUP BY user_id
)
SELECT A.user_id, A.anime_id
FROM `anime-rec-dev.landing_area.favorite_item` A
JOIN user_last_crawl_date B
ON A.user_id = B.user_id AND A.crawl_date = B.last_crawl_date