resource "aws_internet_gateway" "test-igw" {
  vpc_id = aws_vpc.test-vpc.id
  tags = merge(
    {
      "Name" = format("%s-%s-${var.igw_suffix}",var.project_name,var.platforme_name)
    })

}

