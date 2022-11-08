data "aws_availability_zones" "available" {
}
variable "igw_suffix" {
  type    = string
  default = "igw-demo"
}

variable "nat_suffix" {
  type    = string
  default = "nat-demo"
}

variable "elasticache_subnet_group_suffix" {
  type    = string
  default = "elasticache-subnet-group-demo"
}

variable "eip_nat_suffix" {
  type    = string
  default = "eip-demo"
}
variable "public_subnet_suffix" {
  type    = string
  default = "pub-sub-demo"
}

variable "private_subnet_suffix" {
  type    = string
  default = "priv-sub-demo"
}

variable "public_routes_tables_suffix" {
  type    = string
  default = "pub-routes-table-demo"
}

variable "private_routes_tables_suffix" {
  type    = string
  default = "priv-routes-table-demo"
}
variable "ecs_cluster_suffix"{
  type    = string
  default = "ecs-cluster"

}
variable "ecs_service"{
  type    = string
  default = "ecs-service"
}

variable "task_def_suffix" {
  type    = string
  default = "task_definition"
}

variable "alb_suffix" {
 type    = string
 default = "alb"
}
variable "alb_target_group"{
 type    = string
 default = "alb_target_group"
}

data "aws_iam_policy_document" "ecs_task_execution_role" {
  version = "2012-10-17"
  statement {
    sid     = ""
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}


variable "aws_region" {
  default     = "us-east-1"
  description = "aws region where our resources going to create choose"
  #replace the region as suits for your requirement
}

variable "az_count" {
  default     = "2"
  description = "number of availability zones in above region"
}

variable "ecs_task_execution_role" {
  default     = "myECcsTaskExecutionRole"
  description = "ECS task execution role name"
}

variable "app_image" {
  default     = "nginx:latest"
  description = "docker image to run in this ECS cluster"
}

variable "app_port" {
  default     = 80
  description = "portexposed on the docker image"
}

variable "app_count" {
  default     = "2" #choose 2 bcz i have choosen 2 AZ
  description = "numer of docker containers to run"
}

variable "health_check_path" {
  default = "/"
}

variable "fargate_cpu" {
  default     = 256
  description = "fargate instacne CPU units to provision,my requirent 1 vcpu so gave 1024"
}

variable "fargate_memory" {
  default     = 512
  description = "Fargate instance memory to provision (in MiB) not MB"
}
variable "image_tag_mutability" {
  type        = string
  default     = "IMMUTABLE"
  description = "The tag mutability setting for the repository (defaults to IMMUTABLE)"
}

