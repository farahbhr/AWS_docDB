resource "aws_internet_gateway" "default" {
  vpc_id = "${aws_vpc.ssh-tunnel.id}"
  tags = merge(
    {
      "Name" = format("%s-%s-${var.igw_suffix}",var.project_name,var.platforme_name)
    })

}


