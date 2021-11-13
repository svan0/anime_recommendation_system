CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user` AS
WITH ranked_profile_by_crawl_date AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY crawl_date DESC) AS row_number
    FROM `anime-rec-dev.landing_area.profile_item`
)
SELECT uid AS user_id, url AS user_url, last_online_date, num_forum_posts, num_reviews, num_recommendations, num_blog_posts, num_days AS num_days_watching_anime, mean_score, clubs
FROM ranked_profile_by_crawl_date
WHERE row_number = 1