CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user_anime_review` AS
WITH ranked_review_by_crawl_date AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, anime_id ORDER BY crawl_date DESC) AS row_number
    FROM `anime-rec-dev.landing_area.review_item`
)
SELECT user_id, anime_id, uid AS review_id, review_date, num_useful, overall_score, story_score, animation_score, sound_score, character_score, enjoyment_score
FROM ranked_review_by_crawl_date
WHERE row_number = 1
