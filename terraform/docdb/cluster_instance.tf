resource "aws_docdb_cluster_instance" "example" {
  count                      = "${var.docdb_count}"
  cluster_identifier         = "${aws_docdb_cluster.docdb_cluster.id}"
  instance_class             = var.instance_class
  apply_immediately          = var.apply_immediately
  auto_minor_version_upgrade = var.auto_minor_version_upgrade
  engine                     = var.engine

  tags = merge(
    {
      "Name" = format("%s-%s-cluster-instance-%s",var.project_name,var.platforme_name,var.docdb_count)
    })

}

