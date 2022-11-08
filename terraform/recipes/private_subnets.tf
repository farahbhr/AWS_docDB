#################
# Private Subnets
#################
resource "aws_subnet" "private-subnets" {
  count = length(var.private_subnets) > 0  ? length(var.private_subnets) : 0
  vpc_id                          = aws_vpc.main.id
  cidr_block                      = element(concat(var.private_subnets, [""]), count.index)
  availability_zone               = length(regexall("^[a-z]{2}-", element(var.azs, count.index))) > 0 ? element(var.azs, count.index) : null
  availability_zone_id            = length(regexall("^[a-z]{2}-", element(var.azs, count.index))) == 0 ? element(var.azs, count.index) : null

  tags = merge(
    {
      "Name" = format("%s-%s-${var.private_subnet_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })
}