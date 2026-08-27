provider "aws" {
  region = "eu-central-1"
}

module "pipeline" {
  source       = "../../modules/pipeline_base"
  environment  = "dev"
  project_name = "prueba-sgd"
}