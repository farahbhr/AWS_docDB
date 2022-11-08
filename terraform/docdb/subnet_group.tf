resource "aws_docdb_subnet_group" "subnet_group" {
  name = format("%s-%s-subnet-group",var.project_name,var.platforme_name)
  description = "Allowed subnets for DB cluster instances"
  subnet_ids = ["${aws_subnet.ssh-tunnel.id}", "${aws_subnet.ssh-tunnel2.id}"]
  tags = merge(
    {
      "Name" = format("%s-%s-subnet-group",var.project_name,var.platforme_name)
    })
}


