resource "aws_eip" "test-eip" {
  count      = var.az_count
  vpc        = true
  depends_on = [aws_internet_gateway.test-igw]
  tags = merge(
    {
      "Name" = format("%s-%s-${var.eip_nat_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })
}
