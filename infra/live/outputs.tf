# S0: expose the outputs listed in contracts/runtime.yaml under terraform_outputs.
# scripts/register-runner.sh, scripts/catch-up.sh and the grader read them by those names.

output "app_public_ip" {
  description = "Elastic IP of the app host"
  value       = module.app.public_ip
}

output "app_instance_id" {
  description = "EC2 instance ID of the app host"
  value       = module.app.instance_id
}

output "platform_instance_id" {
  description = "EC2 instance ID of the platform host"
  value       = module.platform.instance_id
}

output "platform_private_ip" {
  description = "Private IP of the platform host"
  value       = module.platform.private_ip
}

output "llm_instance_id" {
  description = "EC2 instance ID of the LLM host"
  value       = module.llm.instance_id
}

output "llm_private_ip" {
  description = "Private IP of the LLM host"
  value       = module.llm.private_ip
}

output "rds_endpoint" {
  description = "Endpoint address of the RDS Postgres instance"
  value       = module.rds.endpoint
}

output "artifact_bucket" {
  description = "S3 bucket for MLflow artifacts and training datasets"
  value       = module.artifact_bucket.name
}
