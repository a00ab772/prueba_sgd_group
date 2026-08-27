provider "aws" {
  region = "eu-central-1"
}

module "pipeline" {
  source       = "../../modules/pipeline_base"
  environment  = "prod"
  project_name = "prueba-sgd"
}