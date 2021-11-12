CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user_anime_activity` AS
WITH latest_activity_user_item AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, anime_id ORDER BY date DESC, crawl_date DESC) AS row_number
    FROM `anime-rec-dev.landing_area.activity_item`
)
SELECT user_id, anime_id, activity_type, date AS activity_date
FROM latest_activity_user_item
WHERE row_number = 1
