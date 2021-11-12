CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.anime` AS
WITH ranked_anime_by_crawl_date AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY crawl_date DESC) AS row_number
    FROM `anime-rec-dev.landing_area.anime_item`
)
SELECT  uid AS anime_id,
        url	AS anime_url,
        title
        synopsis,
        main_pic
        type,
        source_type,
        num_episodes,
        status,
        start_date,
        end_date,
        season,
        studios,
        genres,
        score,
        score_count,
        score_rank,
        popularity_rank,
        members_count,
        favorites_count,
        watching_count,
        completed_count,
        on_hold_count,
        dropped_count,
        plan_to_watch_count,
        total_count,
        score_10_count,
        score_09_count,
        score_08_count,
        score_07_count,
        score_06_count,
        score_05_count,
        score_04_count,
        score_03_count,
        score_02_count,
        score_01_count,
        clubs,
        pics
FROM ranked_anime_by_crawl_date
WHERE row_number = 1