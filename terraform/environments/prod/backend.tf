# Configuración vacía. El bucket, la key, la región y la tabla de DynamoDB se completarán con el comando
# `terraform init -backend-config`, así no exfiltramos esa información al repositorio de github.
terraform {
  backend "s3" {}
}
