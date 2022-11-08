#################
# Private Routes
#################
resource "aws_route" "private_internet_gateway" {
  count =  length(var.private_subnets) 

  route_table_id         = element(aws_route_table.private.*.id,count.index)
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = element(aws_nat_gateway.nat.*.id, count.index)
  timeouts {
    create = "5m"
  }
}


