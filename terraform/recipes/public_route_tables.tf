#######################
# Publiс Routes Tables
#######################
resource "aws_route_table" "public" {
  count =  length(var.public_subnets) > 0  ? length(var.public_subnets) : 0

  vpc_id = aws_vpc.main.id
  tags = merge(
    {
      "Name" = format("%s-%s-${var.public_routes_tables_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })   
}

###################################
# Publiс Routes Tables Association
###################################

resource "aws_route_table_association" "public" {
  count =  length(var.public_subnets) > 0 ? length(var.public_subnets) : 0

  subnet_id      = element(aws_subnet.public-subnets.*.id, count.index)
  route_table_id = aws_route_table.public[0].id
}
