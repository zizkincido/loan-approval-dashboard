output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "mart_dataset" {
  value = google_bigquery_dataset.mart.dataset_id
}

output "spark_cluster" {
  value = google_dataproc_cluster.spark_cluster.name
}