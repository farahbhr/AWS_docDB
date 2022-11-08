variable "project_name" {
  type    = string
  default = "tf-initiation"
}

variable "platforme_name" {
  type    = string
  default = "dev"
}

variable "vpc_cidr" {
  type    = string
  default = "192.168.0.0/24"
}

variable "public_subnets" {
  type    = list(string)
  default = ["192.168.0.0/26"]
}

variable "private_subnets" {
  type    = list(string)
  default = ["192.168.0.64/26","192.168.0.128/26","192.168.0.192/26"]
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

variable "azs" {
  type    = list(string)
  default = ["us-east-1a","us-east-1b","us-east-1c"]
}

variable "elasticache_engine" {
  type    = string
  default = "redis"
}

variable "elasticache_node_type" {
  type    = string
  default = "cache.m5.large"
}

variable "elasticache_nodes_number" {
  type    = number
  default = 1
}

variable "ec2_ami" {
  type    = string
  default = "ami-04254e39f96f8f32d"
}

variable "ec2_public_key" {
  type    = string
  default = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQD06O3cH5ffoxuCj32ntzOR5peXAI1dJqmN0DMBwcZ6Fkj4ha6LWUJKd3Q6Vl/izxskocH01eRFn9I1Gtrg5EiKRPNbZnIm+P3dK44+ne8sFyguv9klqdAe2+TaSzDd154ge9J/AdpM8wNHTdu0DEV10v3AxpcYpPjnymirUZk1ZNPKWDrnQyhuEeJtJ1I0vtGbxNYJtUHhnZnG1nzxNmnnLSBkBvlScTfyVXD0WWDgDSSCXd/2EuRORa8BRVAZ3L2XSRqzz8YeccPgwpvcCN5Cm2SCqvDjXkyOi3Vnh1ifrX11WRS7ztELJZ5J8gai0BXVpDrqKVpyVAI1mcsg4fDeFEDwEFsOzwTW0plTZv0hWpNih92IOOJpQn0Su8W+Afib0X4gbcBrPVg+Fs1Oe6bmWnbuAAjYlL1W13SQmhmqoz0ZzBDA388fxoVHZBfeKZ4OUKo9N9rAhFqEv1fnHslW0DRaOEDJ4JpYBUAWNw8FuVDACRIa9xFdPaScXO7bWUc= sinda@sinda"
}

variable "param_group_family" {
  type    = string
  default = "redis6.x"
}

variable "my_ip" {
  type    = string
  default = "197.238.6.33/32"
}

variable "maher_ip" {
  type    = string
  default = "197.0.122.241/32"
}

variable "token_password" {
  type    = string
  default = "auth_token_password"
}