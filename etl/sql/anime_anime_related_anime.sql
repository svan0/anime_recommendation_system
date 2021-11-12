CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.anime_anime_related` AS
WITH src_anime_last_crawl_date AS (
    SELECT src_anime, MAX(crawl_date) AS last_crawl_date
    FROM `anime-rec-dev.landing_area.related_anime_item`
    GROUP BY src_anime
),
undirected_related_anime AS (
    SELECT A.src_anime, A.dest_anime
    FROM `anime-rec-dev.landing_area.related_anime_item` A
    JOIN src_anime_last_crawl_date B
    ON A.src_anime = B.src_anime AND A.crawl_date = B.last_crawl_date 
)
SELECT animeA, animeB
FROM (
    (SELECT src_anime AS animeA, dest_anime AS animeB FROM undirected_related_anime)
    UNION DISTINCT
    (SELECT dest_anime AS animeA, src_anime AS animeB FROM undirected_related_anime)
)