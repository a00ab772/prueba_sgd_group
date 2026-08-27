# MWAA necesita que el bucket tenga el versionado activo
resource "aws_s3_bucket_versioning" "pipeline_bucket_versioning" {
  bucket = aws_s3_bucket.pipeline_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "pipeline_bucket_block" {
  bucket = aws_s3_bucket.pipeline_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "dags_folder" {
  bucket       = aws_s3_bucket.pipeline_bucket.id
  key          = "dags/"
  content_type = "application/x-directory"
}

resource "aws_mwaa_environment" "pipeline_airflow" {
  name              = "${var.project_name}-mwaa-${var.environment}"
  airflow_version   = var.mwaa_airflow_version
  environment_class = var.mwaa_environment_class

  execution_role_arn = aws_iam_role.mwaa_execution_role.arn

  source_bucket_arn = aws_s3_bucket.pipeline_bucket.arn
  dag_s3_path       = "dags"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = [for s in aws_subnet.private : s.id]
  }

  webserver_access_mode = "PUBLIC_ONLY"

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = {
    Name = "${var.project_name}-mwaa-${var.environment}"
  }

  depends_on = [
    aws_s3_bucket_versioning.pipeline_bucket_versioning,
    aws_s3_object.dags_folder,
    aws_security_group_rule.mwaa_self_ingress,
    aws_iam_role_policy.mwaa_execution_policy
  ]
}
