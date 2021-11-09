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

resource "google_storage_bucket_object" "dataflow_ingestion_artifact" {
  name   = "etl/ingestion.py"
  source = "/Users/marouenesfargandoura/Desktop/personal_projects/anime_recommendation_system/etl/ingestion.py"
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

resource "google_pubsub_topic" "data_ingestion_topic" {
  name = "data_ingestion_queue"
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

resource "google_dataflow_job" "data_ingestion_job" {
    name = "data_ingestion_to_bq"
    template_gcs_path = "gs://anime-rec-dev-artifacts/etl/ingestion.py"
    temp_gcs_location = "gs://anime-rec-dev-dataflow-temp/"
    enable_streaming_engine = true
    region = "us-central1"
    zone = "us-central1-a"
    parameters = {
      input_topic = google_pubsub_topic.data_ingestion_topic.id
      window_interval_sec = 90
    }
    on_delete = "cancel"
}








