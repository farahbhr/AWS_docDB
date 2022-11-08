#######################
# Private Routes Tables
#######################
resource "aws_route_table" "private" {
  count =  length(var.private_subnets) 

  vpc_id = aws_vpc.main.id
  tags = merge(
    {
      "Name" = format("%s-%s-${var.private_routes_tables_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })
}


###################################
# Private Routes Tables Association
###################################
resource "aws_route_table_association" "private" {
  count =  length(var.private_subnets) 

  subnet_id      = element(aws_subnet.private-subnets.*.id, count.index)
  route_table_id = element(aws_route_table.private.*.id, count.index)
}
