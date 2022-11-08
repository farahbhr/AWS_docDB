################
# Public Subnets
################
resource "aws_subnet" "public-subnets" {
  count = length(var.public_subnets) > 0  ? length(var.public_subnets) : 0
  vpc_id                          = aws_vpc.main.id
  cidr_block                      = element(concat(var.public_subnets, [""]), count.index)
  availability_zone               = length(regexall("^[a-z]{2}-", element(var.azs, count.index))) > 0 ? element(var.azs, count.index) : null
  availability_zone_id            = length(regexall("^[a-z]{2}-", element(var.azs, count.index))) == 0 ? element(var.azs, count.index) : null

  tags = merge(
    {
      "Name" = format("%s-%s-${var.public_subnet_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })
}



