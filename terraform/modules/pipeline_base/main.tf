resource "aws_s3_bucket" "pipeline_bucket" {
  bucket        = "${var.project_name}-data-${var.environment}"
  force_destroy = true
}

resource "aws_dynamodb_table" "demo" {
  name         = "${var.project_name}-demo-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "raw_demo" {
  name         = "${var.project_name}-raw-demo-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }
}

resource "aws_secretsmanager_secret" "emr_credentials" {
  name = "${var.project_name}-credentials-${var.environment}"
}