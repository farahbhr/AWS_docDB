###################
# Internet Gateway
###################

resource "aws_internet_gateway" "igw" {
  count = length(var.public_subnets) > 0 ? 1 : 0

  vpc_id =  aws_vpc.main.id
  tags = merge(
    {
      "Name" = format("%s-%s-${var.igw_suffix}",var.project_name,var.platforme_name)
    })  
}
