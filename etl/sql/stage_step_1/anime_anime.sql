CREATE TEMPORARY FUNCTION array_intersection(x ANY TYPE, y ANY TYPE) AS (
  (SELECT COUNT(*) 
  FROM UNNEST(x) as xe
  INNER JOIN (SELECT ye FROM UNNEST(y) as ye)
  ON xe = ye)
);
CREATE TEMPORARY FUNCTION array_union(x ANY TYPE, y ANY TYPE) AS (
  (SELECT COUNT(*) + 1e-6
  FROM UNNEST(x) as xe
  FULL OUTER JOIN (SELECT ye FROM UNNEST(y) as ye)
  ON xe = ye)
);

CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.anime_anime` AS
WITH 
recommendation AS (
    SELECT *, 1 AS recommendation FROM `anime-rec-dev.staging_area.anime_anime_recommendation`
),
related AS (
    SELECT *, 1 AS related FROM `anime-rec-dev.staging_area.anime_anime_related`
),
related_and_recommendation AS (
    SELECT COALESCE(A.animeA, B.animeA) AS animeA,
           COALESCE(A.animeB, B.animeB) AS animeB,
           COALESCE(A.recommendation, 0) AS recommendation,
           A.recommendation_url,
           A.num_recommenders,
           COALESCE(B.related, 0) AS related
    FROM recommendation A
    FULL OUTER JOIN related B
    ON A.animeA = B.animeA AND A.animeB = B.animeB
),
all_anime_genre_club_IOU AS (
    SELECT  A.anime_id AS animeA,
            B.anime_id AS animeB,
            array_intersection(A.genres, B.genres) / array_union(A.genres, B.genres) AS genre_IOU,
            array_intersection(A.clubs, B.clubs) / array_union(A.clubs, B.clubs) AS club_IOU
    FROM `anime-rec-dev.staging_area.anime` A
    CROSS JOIN `anime-rec-dev.staging_area.anime` B
    WHERE A.anime_id != B.anime_id
),
related_and_recommendation_and_genre_club_IOU AS (
    SELECT COALESCE(A.animeA, B.animeA) AS animeA,
           COALESCE(A.animeB, B.animeB) AS animeB, 
           COALESCE(A.recommendation, 0) AS recommendation,
           A.recommendation_url,
           A.num_recommenders,
           COALESCE(A.related, 0) AS related,
           B.genre_IOU,
           B.club_IOU
    FROM related_and_recommendation A
    FULL OUTER JOIN all_anime_genre_club_IOU B
    ON A.animeA = B.animeA AND A.animeB = B.animeB
)
SELECT * FROM related_and_recommendation_and_genre_club_IOU