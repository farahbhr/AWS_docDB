resource "aws_subnet" "ssh-tunnel" {
  vpc_id                  = "${aws_vpc.ssh-tunnel.id}"
  cidr_block              = "192.168.0.0/26"
  map_public_ip_on_launch = true
  availability_zone       = "${data.aws_availability_zones.available.names[0]}"
  tags = merge(
    {
      "Name" = format("%s-%s-${var.public_subnet_suffix}-1",var.project_name,var.platforme_name)
    })

}

resource "aws_subnet" "ssh-tunnel2" {
  vpc_id                  = "${aws_vpc.ssh-tunnel.id}"
  cidr_block              = "192.168.0.64/26"
  map_public_ip_on_launch = true
  availability_zone       = "${data.aws_availability_zones.available.names[1]}"
  tags = merge(
    {
      "Name" = format("%s-%s-${var.public_subnet_suffix}-2",var.project_name,var.platforme_name)
    })

}


