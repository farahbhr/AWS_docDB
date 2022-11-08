data "aws_availability_zones" "available" {}

variable "project_name" {
  type    = string
  default = "tf-initiation"
}

variable "platforme_name" {
  type    = string
  default = "dev"
}
variable "igw_suffix" {
  type    = string
  default = "igw-demo"
}
variable "public_subnet_suffix" {
  type    = string
  default = "pub-sub-demo"
}


variable "docdb_name" {
  default = "docdb-cluster-demo"
}

variable "docdb_sg_name" {
  default = "docdb-sg"
}

variable "docdb_count" {
  default = 1
}
variable "username" {
  type    = string
  default = "imen"
}

variable "password" {
  type    = string
  default = "imen12345"
}

variable "ec2_ami" {
  type    = string
  default = "ami-0e011417bd70948da"
}

variable "public_key_path" {
  default = "~/.ssh/id_rsa.pub"
}

variable "private_key_path" {
  default = "~/.ssh/id_rsa"
}

variable "key_name" {
  default = "ssh-tunnel"
}

variable "snapshot_identifier" {
  type        = string
  default     = ""
  description = "Specifies whether or not to create this cluster from a snapshot. You can use either the name or ARN when specifying a DB cluster snapshot, or the ARN when specifying a DB snapshot"
}

variable "db_port" {
  type        = number
  default     = 27017
  description = "DocumentDB port"
}

variable "retention_period" {
  type        = number
  default     = 5
  description = "Number of days to retain backups for"
}

variable "preferred_backup_window" {
  type        = string
  default     = "07:00-09:00"
  description = "Daily time range during which the backups happen"
}

variable "preferred_maintenance_window" {
  type        = string
  default     = "Mon:22:00-Mon:23:00"
  description = "The window to perform maintenance in. Syntax: `ddd:hh24:mi-ddd:hh24:mi`."
}

variable "engine" {
  type        = string
  default     = "docdb"
  description = "The name of the database engine to be used for this DB cluster. Defaults to `docdb`. Valid values: `docdb`"
}

variable "engine_version" {
  type        = string
  default     = "3.6.0"
  description = "The version number of the database engine to use"
}


variable "skip_final_snapshot" {
  type        = bool
  description = "Determines whether a final DB snapshot is created before the DB cluster is deleted , if true is specified, no DB snapshot is created. If false is specified, a DB snapshot is created before the DB cluster is deleted "
  default     = true
}

variable "enabled_cloudwatch_logs_exports" {
  type        = list(string)
  description = "List of log types to export to cloudwatch. The following log types are supported: `audit`, `error`, `general`, `slowquery`"
  default     = []
}

variable "deletion_protection" {
  type        = bool
  description = "A value that indicates whether the DB cluster has deletion protection enabled"
  default     = false
}

variable "apply_immediately" {
  type        = bool
  description = "Specifies whether any cluster modifications are applied immediately, or during the next maintenance window"
  default     = true
}


variable "storage_encrypted" {
  type        = bool
  description = "Specifies whether the DB cluster is encrypted"
  default     = true
}

variable "family" {
  default     = "docdb3.6"
  description = "Version of docdb family being created"
  type        = string
}

variable "parameters" {
  description = "additional parameters modified in parameter group"
  type        = list(map(any))
  default     = []
}
variable "name" {
  type = string
  default = "parameteres"
}

variable "auto_minor_version_upgrade" {
  type        = bool
  description = "Specifies whether any minor engine upgrades will be applied automatically to the DB instance during the maintenance window or not"
  default     = true
}

variable "instance_class" {
  type        = string
  default     = "db.t3.medium"
  description = "The instance class to use."
}


variable "instance_type" {
  type        = string
  default     = "t4g.nano"
  description = "The instance type to use."
}


