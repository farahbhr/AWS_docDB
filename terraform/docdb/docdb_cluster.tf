resource "aws_docdb_cluster" "docdb_cluster" {
  cluster_identifier                  = "${var.docdb_name}"
  engine                              = var.engine
  master_username                     = var.username
  master_password                     = var.password
  vpc_security_group_ids              = ["${aws_security_group.docdb_sg.id}"]
  db_subnet_group_name                = "${aws_docdb_subnet_group.subnet_group.name}"

  backup_retention_period             = var.retention_period
  preferred_backup_window             = var.preferred_backup_window
  preferred_maintenance_window        = var.preferred_maintenance_window
  skip_final_snapshot                 = var.skip_final_snapshot
  deletion_protection                 = var.deletion_protection
  apply_immediately                   = var.apply_immediately
  storage_encrypted                   = var.storage_encrypted
  port                                = var.db_port
  snapshot_identifier                 = var.snapshot_identifier
  engine_version                      = var.engine_version
  enabled_cloudwatch_logs_exports     = var.enabled_cloudwatch_logs_exports
  db_cluster_parameter_group_name     = aws_docdb_cluster_parameter_group.docdb.name




  tags = merge(
    {
      "Name" = format("%s-%s-docdb-cluster",var.project_name,var.platforme_name)
    })

}
