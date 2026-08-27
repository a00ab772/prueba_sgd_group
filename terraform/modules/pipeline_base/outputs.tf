output "s3_bucket_arn" {
  value = aws_s3_bucket.pipeline_bucket.arn
}

output "s3_bucket_name" {
  value = aws_s3_bucket.pipeline_bucket.id
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.demo.name
}

output "raw_dynamodb_table_name" {
  value = aws_dynamodb_table.raw_demo.name
}

output "secret_arn" {
  value = aws_secretsmanager_secret.emr_credentials.arn
}

output "vpc_id" {
  value = aws_vpc.pipeline_vpc.id
}

output "mwaa_arn" {
  value = aws_mwaa_environment.pipeline_airflow.arn
}

output "mwaa_webserver_url" {
  value = aws_mwaa_environment.pipeline_airflow.webserver_url
}

output "emr_cluster_id" {
  value = aws_emr_cluster.spark_cluster.id
}

output "emr_master_public_dns" {
  value = aws_emr_cluster.spark_cluster.master_public_dns
}