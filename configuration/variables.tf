# set in terraform.tfvars (excluded from source control using .gitignore)
variable "aws_access_key" {
  description = "AWS access key"
}

variable "unique_name" {
  description = "Naming convention for your s3 bucket prefix"
}
