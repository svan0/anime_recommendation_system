terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
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

resource "google_storage_bucket_object" "scheduler_cloud_functions_artifact" {
  name   = "functions/scheduler"
  source = "terraform_gcs_test.txt"
  bucket = google_storage_bucket.artifacts_bucket.name
}

resource "google_sql_database_instance" "crawl_scheduler_db_instance" {
  name             = "scheduler-db-instance-dev-small-2"
  region           = "us-central1"
  database_version = "POSTGRES_13"
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
  name     = "root"
  instance = google_sql_database_instance.crawl_scheduler_db_instance.name
  password = "1123581321"
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







