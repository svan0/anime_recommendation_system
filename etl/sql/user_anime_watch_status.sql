CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user_anime_watch_status` AS
WITH ranked_watch_status_by_crawl_date AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, anime_id ORDER BY crawl_date DESC) AS row_number
    FROM `anime-rec-dev.landing_area.watch_status_item`
)
SELECT user_id, anime_id, status, score, progress
FROM ranked_watch_status_by_crawl_date
WHERE row_number = 1