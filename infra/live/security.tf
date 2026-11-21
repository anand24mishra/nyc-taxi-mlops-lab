# S0: security groups and rules.
#
# Decision point: who may reach the app (ingress scope)? Record it in decisions/.
# Traffic between hosts (ports in contracts/runtime.yaml):
#   app -> platform  MLflow (port 5000)
#   app, platform -> llm  chat and embeddings (ports 8080, 8081)
#   app, platform -> rds  Postgres (port 5432)
# There is no SSH in this lab: use SSM Session Manager for a shell and port forwarding for UIs.

resource "aws_security_group" "app" {
  name        = "lab-app"
  description = "App host (Traefik, app backend, serving, mcp)"
  vpc_id      = data.aws_vpc.default.id

  tags = { Name = "lab-app" }
}

resource "aws_vpc_security_group_ingress_rule" "app_http" {
  count             = length(var.ingress_cidrs)
  security_group_id = aws_security_group.app.id
  description       = "Traefik HTTP from allowed ingress CIDRs"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  cidr_ipv4         = var.ingress_cidrs[count.index]
}

resource "aws_vpc_security_group_egress_rule" "app_all" {
  security_group_id = aws_security_group.app.id
  description       = "All outbound traffic"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_security_group" "platform" {
  name        = "lab-platform"
  description = "Platform host (Dagster, MLflow)"
  vpc_id      = data.aws_vpc.default.id

  tags = { Name = "lab-platform" }
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow" {
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from app host"
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
  referenced_security_group_id = aws_security_group.app.id
}

resource "aws_vpc_security_group_egress_rule" "platform_all" {
  security_group_id = aws_security_group.platform.id
  description       = "All outbound traffic"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_security_group" "llm" {
  name        = "lab-llm"
  description = "LLM host (chat on 8080, embed on 8081)"
  vpc_id      = data.aws_vpc.default.id

  tags = { Name = "lab-llm" }
}

resource "aws_vpc_security_group_ingress_rule" "llm_chat_app" {
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM chat from app host"
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
  referenced_security_group_id = aws_security_group.app.id
}

resource "aws_vpc_security_group_ingress_rule" "llm_embed_app" {
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM embed from app host"
  ip_protocol                  = "tcp"
  from_port                    = 8081
  to_port                      = 8081
  referenced_security_group_id = aws_security_group.app.id
}

resource "aws_vpc_security_group_ingress_rule" "llm_chat_platform" {
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM chat from platform host"
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
  referenced_security_group_id = aws_security_group.platform.id
}

resource "aws_vpc_security_group_ingress_rule" "llm_embed_platform" {
  security_group_id            = aws_security_group.llm.id
  description                  = "LLM embed from platform host"
  ip_protocol                  = "tcp"
  from_port                    = 8081
  to_port                      = 8081
  referenced_security_group_id = aws_security_group.platform.id
}

resource "aws_vpc_security_group_egress_rule" "llm_all" {
  security_group_id = aws_security_group.llm.id
  description       = "All outbound traffic"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_security_group" "rds" {
  name        = "lab-rds"
  description = "RDS Postgres (port 5432)"
  vpc_id      = data.aws_vpc.default.id

  tags = { Name = "lab-rds" }
}

resource "aws_vpc_security_group_ingress_rule" "rds_platform" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from platform host"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.platform.id
}

resource "aws_vpc_security_group_ingress_rule" "rds_app" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from app host"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.app.id
}

resource "aws_vpc_security_group_egress_rule" "rds_all" {
  security_group_id = aws_security_group.rds.id
  description       = "All outbound traffic"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}
