##############
# EC2 Key Pair
##############
resource "aws_key_pair" "ec2-key-pair" {
    key_name = "ec2-key-pair"
    public_key = var.ec2_public_key
    tags = merge(
    {
      "Name" = format("%s-%s-ec2-key-pair",var.project_name,var.platforme_name)
    })
}
