provider "aws" {
  region = "eu-central-1"
}

module "pipeline" {
  source       = "../../modules/pipeline_base"
  environment  = "dev"
  project_name = "prueba-sgd"
}

output "s3_bucket_name" {
  value = module.pipeline.s3_bucket_name
}
