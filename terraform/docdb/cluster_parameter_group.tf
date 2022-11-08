
resource "aws_docdb_cluster_parameter_group" "docdb" {
  family      = var.family
  name_prefix = var.name
  description = "${var.name} docdb cluster parameter group"
  dynamic "parameter" {
    for_each = var.parameters
    content {
      name  = parameter.value.name
      value = parameter.value.value
    }
  }
}
