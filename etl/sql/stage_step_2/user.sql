CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user` AS
WITH 
external_user AS (
    SELECT  username AS user_id,
            last_online AS last_online_date,
            user_days_spent_watching AS num_days_watching_anime,
            stats_mean_score AS mean_score
    FROM `anime-rec-dev.external_data.user`
),
crawled_user AS (
    SELECT  user_id,
            last_online_date, 
            num_forum_posts, 
            num_reviews, 
            num_recommendations, 
            num_blog_posts, 
            num_days_watching_anime, 
            mean_score
    FROM `anime-rec-dev.staging_area.user`
)
SELECT  COALESCE(A.user_id, B.user_id) AS user_id,
        COALESCE(GREATEST(A.last_online_date, B.last_online_date), A.last_online_date, B.last_online_date) AS last_online_date,
        A.num_forum_posts,
        A.num_reviews,
        A.num_recommendations,
        A.num_blog_posts,
        COALESCE(GREATEST(A.num_days_watching_anime, B.num_days_watching_anime), A.num_days_watching_anime, B.num_days_watching_anime) AS num_days_watching_anime,
        COALESCE(A.mean_score, B.mean_score) AS mean_score
FROM crawled_user A
FULL OUTER JOIN external_user B
ON A.user_id = B.user_id