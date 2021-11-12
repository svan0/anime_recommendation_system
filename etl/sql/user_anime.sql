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

CREATE OR REPLACE TABLE `anime-rec-dev.staging_area.user_anime` AS
WITH 
favorite AS (
    SELECT *, 1 AS favorite FROM `anime-rec-dev.staging_area.user_anime_favorite`
)
SELECT  COALESCE(A.user_id, B.user_id, C.user_id, D.user_id) AS user_id,
        COALESCE(A.anime_id, B.anime_id, C.anime_id, D.anime_id) AS anime_id,
        COALESCE(B.favorite, 0) AS favorite,
        A.review_id,
        A.review_date,
        A.num_useful,
        A.overall_score,
        A.story_score, 
        A.animation_score, 
        A.sound_score, 
        A.character_score, 
        A.enjoyment_score,
        C.activity_type,
        C.activity_date,
        D.status,
        D.score,
        D.progress
FROM `anime-rec-dev.staging_area.user_anime_review` A
FULL OUTER JOIN favorite B
ON A.user_id = B.user_id AND A.anime_id = B.anime_id
FULL OUTER JOIN `anime-rec-dev.staging_area.user_anime_activity` C
ON COALESCE(A.user_id, B.user_id) = C.user_id AND COALESCE(A.anime_id, B.anime_id) = C.anime_id
FULL OUTER JOIN `anime-rec-dev.staging_area.user_anime_watch_status` D
ON COALESCE(A.user_id, B.user_id, C.user_id) = D.user_id AND COALESCE(A.anime_id, B.anime_id, C.anime_id) = D.anime_id