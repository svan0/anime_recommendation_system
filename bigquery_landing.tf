resource "google_bigquery_dataset" "landing_area" {
  dataset_id = "landing_area"
  location = "us-central1"
}

resource "google_bigquery_table" "anime_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "anime_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "uid",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "url",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "title",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "synopsis",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "main_pic",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "type",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "source_type",
      "type" : "STRING"
    },
    {
      "mode" : "NULLABLE",
      "name" : "num_episodes",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "status",
      "type" : "STRING"
    },
    {
      "mode" : "NULLABLE",
      "name" : "start_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "NULLABLE",
      "name" : "end_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "NULLABLE",
      "name" : "season",
      "type" : "STRING"
    },
    {
      "mode" : "REPEATED",
      "name" : "studios",
      "type" : "STRING"
    },
    {
      "mode" : "REPEATED",
      "name" : "genres",
      "type" : "STRING"
    },
    {
      "mode" : "NULLABLE",
      "name" : "score",
      "type" : "FLOAT64"
    },
    {
      "mode" : "NULLABLE",
      "name" : "score_count",
      "type" : "INT64"
    },
    {
      "mode" : "NULLABLE",
      "name" : "score_rank",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "popularity_rank",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "members_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "favorites_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "watching_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "completed_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "on_hold_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "dropped_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "plan_to_watch_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "total_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_10_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_09_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_08_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_07_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_06_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_05_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_04_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_03_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_02_count",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "score_01_count",
      "type" : "INT64"
    },
    {
      "mode" : "REPEATED",
      "name" : "clubs",
      "type" : "STRING"
    },
    {
      "mode" : "REPEATED",
      "name" : "pics",
      "type" : "STRING"
    }
  ]
  EOF
}
resource "google_bigquery_table" "profile_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "profile_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "uid",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "url",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "last_online_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_forum_posts",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_reviews",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_recommendations",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_blog_posts",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_days",
      "type" : "FLOAT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "mean_score",
      "type" : "FLOAT64"
    },
    {
      "mode" : "REPEATED",
      "name" : "clubs",
      "type" : "STRING"
    }
  ]
  EOF
}

resource "google_bigquery_table" "review_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "review_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "uid",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "url",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "anime_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "user_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "review_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_useful",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "overall_score",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "story_score",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "animation_score",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "sound_score",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "character_score",
      "type" : "INT64"
    },
    {
      "mode" : "REQUIRED",
      "name" : "enjoyment_score",
      "type" : "INT64"
    }
  ]
  EOF
}
resource "google_bigquery_table" "watch_status_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "watch_status_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "user_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "anime_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "status",
      "type" : "STRING"
    },
    {
      "mode" : "NULLABLE",
      "name" : "score",
      "type" : "INT64"
    },
    {
      "mode" : "NULLABLE",
      "name" : "progress",
      "type" : "INT64"
    }
  ]
  EOF
}
resource "google_bigquery_table" "favorite_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "favorite_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "user_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "anime_id",
      "type" : "STRING"
    }
  ]
  EOF
}
resource "google_bigquery_table" "activity_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "activity_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "user_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "anime_id",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "activity_type",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "date",
      "type" : "DATETIME"
    }
  ]
  EOF
}

resource "google_bigquery_table" "related_anime_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "related_anime_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "src_anime",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "dest_anime",
      "type" : "STRING"
    }
  ]
  EOF
}
resource "google_bigquery_table" "recommendation_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "recommendation_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "url",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "src_anime",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "dest_anime",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "num_recs",
      "type" : "INT64"
    }
  ]
  EOF
}

resource "google_bigquery_table" "friends_item" {
  dataset_id = google_bigquery_dataset.landing_area.dataset_id
  table_id = "friends_item"
  schema = <<EOF
  [
    {
      "mode" : "REQUIRED",
      "name" : "crawl_date",
      "type" : "DATETIME"
    },
    {
      "mode" : "REQUIRED",
      "name" : "src_profile",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "dest_profile",
      "type" : "STRING"
    },
    {
      "mode" : "REQUIRED",
      "name" : "friendship_date",
      "type" : "DATETIME"
    }
  ]
  EOF
}