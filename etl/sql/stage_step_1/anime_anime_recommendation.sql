CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.anime_anime_recommendation` AS
WITH src_anime_last_crawl_date AS (
    SELECT src_anime, MAX(crawl_date) AS last_crawl_date
    FROM `anime-rec-dev.landing_area.recommendation_item`
    GROUP BY src_anime
),
undirected_recommendation AS (
    SELECT A.src_anime, A.dest_anime, A.url, A.num_recs
    FROM `anime-rec-dev.landing_area.recommendation_item` A
    JOIN src_anime_last_crawl_date B
    ON A.src_anime = B.src_anime AND A.crawl_date = B.last_crawl_date 
)
SELECT animeA, animeB, url AS recommendation_url, num_recs AS num_recommenders
FROM (
    (SELECT src_anime AS animeA, dest_anime AS animeB, url, num_recs FROM undirected_recommendation)
    UNION DISTINCT
    (SELECT dest_anime AS animeA, src_anime AS animeB, url, num_recs FROM undirected_recommendation)
)