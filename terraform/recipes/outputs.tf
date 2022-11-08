output "ec2_ip_address" {
  value = aws_instance.ec2.public_ip
}

output "elasticache-config-endpoint" {
  value = aws_elasticache_replication_group.elasticache-replication-group.configuration_endpoint_address
}

output "elasticache-primary-endpoint" {
  value = aws_elasticache_replication_group.elasticache-replication-group.primary_endpoint_address
}
