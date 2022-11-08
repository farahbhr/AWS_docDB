resource "aws_nat_gateway" "test-natgw" {
  count         = var.az_count
  subnet_id     = element(aws_subnet.public.*.id, count.index)
  allocation_id = element(aws_eip.test-eip.*.id, count.index)
 tags = merge(
    {
      "Name" = format("%s-%s-${var.nat_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })

}
