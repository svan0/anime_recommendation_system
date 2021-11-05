terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.90.0"
    }
  }
}

provider "google" {
  credentials = file("/Users/marouenesfargandoura/Downloads/anime-rec-dev-1b9c92c28623.json")
  project     = "anime-rec-dev"
  region      = "us-central1"
  zone        = "us-central-c"
}

resource "google_storage_bucket" "artifacts_bucket" {
  name = "anime-rec-dev-artifacts"
}
resource "google_storage_bucket" "anime_images_bucket" {
  name = "anime-rec-dev-images"
}
resource "google_storage_bucket" "dataflow_temp" {
  name          = "anime-rec-dev-dataflow-temp"
  location      = "us-central1"
}
data "archive_file" "scheduler_zip" {
  type = "zip"
  source_dir = "/Users/marouenesfargandoura/Desktop/personal_projects/anime_recommendation_system/crawl_scheduler"
  output_path = "/tmp/function-scheduler.zip"
}

resource "google_storage_bucket_object" "scheduler_cloud_functions_artifact" {
  name   = "functions/scheduler_${data.archive_file.scheduler_zip.output_md5}.zip"
  source = data.archive_file.scheduler_zip.output_path
  bucket = google_storage_bucket.artifacts_bucket.name
}

resource "google_sql_database_instance" "crawl_scheduler_db_instance" {
  name             = "scheduler-db-instance-dev-small-3"
  region           = "us-central1"
  database_version = "POSTGRES_11"
  settings {
    availability_type = "ZONAL"
    tier              = "db-custom-1-3840"
  }
}
resource "google_sql_database" "crawl_scheduler_db" {
  name     = "scheduler_db"
  instance = google_sql_database_instance.crawl_scheduler_db_instance.name
}
resource "google_sql_user" "user" {
  name     = "svanO"
  instance = google_sql_database_instance.crawl_scheduler_db_instance.name
  password = "1123581321"
}

resource "google_pubsub_topic" "anime_crawl_topic" {
  name = "anime_crawl_queue"
}
resource "google_pubsub_topic" "profile_crawl_topic" {
  name = "profile_crawl_queue"
}

resource "google_pubsub_topic" "anime_data_topic" {
  name = "anime_data_ingestion"
}
resource "google_pubsub_topic" "profile_data_topic" {
  name = "profile_data_ingestion"
}
resource "google_pubsub_topic" "review_data_topic" {
  name = "review_data_ingestion"
}
resource "google_pubsub_topic" "watch_status_data_topic" {
  name = "watch_status_data_ingestion"
}
resource "google_pubsub_topic" "favorite_data_topic" {
  name = "favorite_data_ingestion"
}
resource "google_pubsub_topic" "activity_data_topic" {
  name = "activity_data_ingestion"
}
resource "google_pubsub_topic" "related_anime_data_topic" {
  name = "related_anime_data_ingestion"
}
resource "google_pubsub_topic" "recommendation_anime_data_topic" {
  name = "recommendation_anime_data_ingestion"
}

resource "google_cloudfunctions_function" "anime_crawl_scheduler" {
  name        = "anime_crawl_scheduler"
  description = "Inserts next anime urls to crawl in anime crawl pubsub topic"
  runtime     = "python38"

  available_memory_mb   = 128
  source_archive_bucket = google_storage_bucket.artifacts_bucket.name
  source_archive_object = google_storage_bucket_object.scheduler_cloud_functions_artifact.name
  trigger_http          = true
  timeout               = 60
  entry_point           = "schedule_anime"

  environment_variables = {
    PROJECT_ID="anime-rec-dev"
    SCHEDULER_DB_INSTANCE = "anime-rec-dev:us-central1:scheduler-db-instance-dev-small-3"
    SCHEDULER_DB = "scheduler_db"
    SCHEDULER_DB_USER = "postgres"
    SCHEDULER_DB_PASSWORD = "1123581321"
    SCHEDULE_ANIME_PUBSUB_TOPIC = "anime_crawl_queue"
  }
}
resource "google_cloudfunctions_function" "profile_crawl_scheduler" {
  name        = "profile_crawl_scheduler"
  description = "Inserts next profile urls to crawl in profile crawl pubsub topic"
  runtime     = "python38"

  available_memory_mb   = 128
  source_archive_bucket = google_storage_bucket.artifacts_bucket.name
  source_archive_object = google_storage_bucket_object.scheduler_cloud_functions_artifact.name
  trigger_http          = true
  timeout               = 60
  entry_point           = "schedule_profile"

  environment_variables = {
    PROJECT_ID="anime-rec-dev"
    SCHEDULER_DB_INSTANCE = "anime-rec-dev:us-central1:scheduler-db-instance-dev-small-3"
    SCHEDULER_DB = "scheduler_db"
    SCHEDULER_DB_USER = "postgres"
    SCHEDULER_DB_PASSWORD = "1123581321"
    SCHEDULE_PROFILE_PUBSUB_TOPIC = "profile_crawl_queue"
  }
}

resource "google_pubsub_subscription" "anime_crawl_subscription" {
  name = "anime_crawl_subscription"
  topic = google_pubsub_topic.anime_crawl_topic.name
  message_retention_duration = "604800s"
  ack_deadline_seconds = 300
}
resource "google_pubsub_subscription" "profile_crawl_subscription" {
  name = "profile_crawl_subscription"
  topic = google_pubsub_topic.profile_crawl_topic.name
  message_retention_duration = "604800s"
  ack_deadline_seconds = 300
}


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

resource "google_dataflow_job" "anime_data_to_bq"{
  name = "anime_data_to_bq"
  template_gcs_path = "gs://dataflow-templates/latest/PubSub_to_BigQuery"
  temp_gcs_location = google_storage_bucket.dataflow_temp.url
  enable_streaming_engine = true
  region = "us-central1"
  zone = "us-central1-a"
  parameters = {
    inputTopic = google_pubsub_topic.anime_data_topic.id
    outputTableSpec = "anime-rec-dev:landing_area.anime_item"
  }
}





