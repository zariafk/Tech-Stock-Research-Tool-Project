terraform {
  backend "s3" {
    bucket  = "c22-tsrt-terraform-state" 
    key     = "global/<insert-your-path>/terraform.tfstate" # Insert your path here, e.g., "envs/prod/terraform.tfstate"
    region  = "eu-west-2"
    encrypt = true
  }
}