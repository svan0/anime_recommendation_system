CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user_anime` AS
WITH 
external_user_anime AS (
    SELECT  username AS user_id,
            anime_id,
            my_score AS overall_score,
            CASE my_status
                WHEN "1" THEN "watching"
                WHEN "2" THEN "completed"
                WHEN "3" THEN "on_hold"
                WHEN "4" THEN "dropped"
                WHEN "6" THEN "plan_to_watch"
                ELSE NULL
            END AS status,
            my_watched_episodes AS progress,
            CAST(my_last_updated AS DATETIME) AS last_interaction_date
    FROM `anime-rec-dev.external_data.user_anime`
),
crawled_user_anime AS (
    SELECT  user_id,
            anime_id,
            favorite,
            COALESCE(overall_score, score) AS overall_score,
            story_score,
            animation_score, 
            sound_score, 
            character_score, 
            enjoyment_score,
            status,
            progress,
            COALESCE(GREATEST(activity_date, review_date), activity_date, review_date) AS last_interaction_date,
    FROM `anime-rec-dev.staging_area.user_anime`
    WHERE overall_score IS NULL OR score IS NULL OR overall_score = score
)
SELECT  COALESCE(A.user_id, B.user_id) AS user_id,
        COALESCE(A.anime_id, B.anime_id) AS anime_id,
        COALESCE(A.favorite, 0) AS favorite,
        COALESCE(A.overall_score, B.overall_score) AS overall_score,
        A.story_score,
        A.animation_score, 
        A.sound_score, 
        A.character_score, 
        A.enjoyment_score,
        COALESCE(A.status, B.status) AS status,
        COALESCE(A.progress, B.progress) AS progress,
        COALESCE(GREATEST(A.last_interaction_date, B.last_interaction_date), A.last_interaction_date, B.last_interaction_date) AS last_interaction_date
FROM crawled_user_anime A
FULL OUTER JOIN external_user_anime B
ON A.user_id = B.user_id AND A.anime_id = B.anime_id


