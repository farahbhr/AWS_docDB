##############
# EC2 Instance
##############

resource "aws_instance" "ec2" {
    ami =  var.ec2_ami
    subnet_id   = aws_subnet.public-subnets[0].id
    instance_type = "t3.nano"
    vpc_security_group_ids = [aws_security_group.ec2-sg.id]
    key_name = aws_key_pair.ec2-key-pair.key_name
    associate_public_ip_address = true
    tags = merge(
    {
      "Name" = format("%s-%s-ec2-insttance}",var.project_name,var.platforme_name)
    })
}