provider "aws" {
  region = "us-east-1"
}

module "pipeline" {
  source       = "../../modules/pipeline_base"
  environment  = "dev"
  project_name = "prueba-sgd"
}