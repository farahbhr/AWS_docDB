output "docdb_cluster_instances" {
  value = "${aws_docdb_cluster.docdb_cluster.cluster_members}"
}

output "docdb_cluster_endpoint" {
  value = "${aws_docdb_cluster.docdb_cluster.endpoint}"
}

output "SSH_Port_Forward_Command" {
  value = "ssh -L8182:${aws_docdb_cluster.docdb_cluster.endpoint}:8182 ec2-user@${aws_instance.docdb-ec2-connector.public_ip}"
}
