resource "aws_docdb_subnet_group" "example" {
  name = format("%s-%s-docdb-subnet-group",var.project_name,var.platforme_name)
  subnet_ids = ["${aws_subnet.ssh-tunnel.id}", "${aws_subnet.ssh-tunnel2.id}"]

  tags = merge(
    {
      "Name" = format("%s-%s-docdb-subnet-group",var.project_name,var.platforme_name)
    })

}


