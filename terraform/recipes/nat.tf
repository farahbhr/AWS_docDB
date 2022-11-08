##############
# NAT Gateway
##############

#Elastic IP
resource "aws_eip" "eip_nat" {
  count =  length(var.public_subnets) 
  vpc = true

  tags = merge(
    {
      "Name" = format("%s-%s-${var.eip_nat_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))
    })
}


resource "aws_nat_gateway" "nat" {
  count =  length(var.public_subnets)  

  allocation_id = element(aws_eip.eip_nat.*.id, count.index)
  subnet_id = element(aws_subnet.public-subnets.*.id,count.index)

  tags = merge(
    {
      "Name" = format("%s-%s-${var.nat_suffix}-%s",var.project_name,var.platforme_name,element(var.azs, count.index))  
    })
  depends_on = [aws_internet_gateway.igw]
}
