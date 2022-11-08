resource "aws_subnet" "private" {
  count             = var.az_count
  cidr_block        = cidrsubnet(aws_vpc.test-vpc.cidr_block, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  vpc_id            = aws_vpc.test-vpc.id
    tags = merge(
    {
      "Name" = format("%s-%s-${var.public_subnet_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })

}

resource "aws_subnet" "public" {
  count                   = var.az_count
  cidr_block              = cidrsubnet(aws_vpc.test-vpc.cidr_block, 8, var.az_count + count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  vpc_id                  = aws_vpc.test-vpc.id
  map_public_ip_on_launch = true
  tags = merge(
    {
      "Name" = format("%s-%s-${var.private_subnet_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })

}
