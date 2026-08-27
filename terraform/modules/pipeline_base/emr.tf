resource "aws_emr_cluster" "spark_cluster" {
  name          = "${var.project_name}-emr-${var.environment}"
  release_label = var.emr_release_label
  applications  = ["Spark"]

  service_role = aws_iam_role.emr_service_role.arn
  log_uri      = "s3://${aws_s3_bucket.pipeline_bucket.id}/emr-logs/"

  ec2_attributes {
    subnet_id                         = aws_subnet.private[0].id
    instance_profile                  = aws_iam_instance_profile.emr_instance_profile.arn
    additional_master_security_groups = aws_security_group.emr.id
    additional_slave_security_groups  = aws_security_group.emr.id
  }

  master_instance_group {
    instance_type = var.emr_master_instance_type
  }

  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count
  }

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.dynamicAllocation.enabled" = "true"
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-emr-${var.environment}"
  }

  depends_on = [
    aws_nat_gateway.pipeline_nat,
    aws_iam_role_policy_attachment.emr_service_role_policy,
    aws_iam_role_policy_attachment.emr_instance_role_policy
  ]
}
