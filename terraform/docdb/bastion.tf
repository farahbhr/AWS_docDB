resource "aws_instance" "docdb-ec2-connector" {
  connection {
    user        = "ec2-user"
    agent       = "false"
    private_key = "${file(var.private_key_path)}"
  }

  instance_type          = var.instance_type
  ami                    = var.ec2_ami
  key_name               = "${aws_key_pair.auth.id}"
  vpc_security_group_ids = ["${aws_security_group.bastion_sg.id}"]
  subnet_id              = "${aws_subnet.ssh-tunnel.id}"
  tags = merge(
    {
      "Name" = format("%s-%s-bastion-instance",var.project_name,var.platforme_name)
    })


}


