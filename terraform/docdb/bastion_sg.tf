resource "aws_security_group" "bastion_sg" {
  name = format("%s-%s-bastion-sg",var.project_name,var.platforme_name)
  description = "Bastion"
  vpc_id      = "${aws_vpc.ssh-tunnel.id}"
 ingress {
    from_port = 8182
    to_port   = 8182
    protocol  = "tcp"
    self      = true
  }

  ingress {
    from_port = 27017
    to_port   = 27017
    protocol  = "tcp"
    self      = true
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${local.workstation-external-cidr}"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      "Name" = format("%s-%s-bastion-sg",var.project_name,var.platforme_name)
    })

}



