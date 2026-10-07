variable "region" {
  description = "AWS region. The project pins us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Must match PROJECT_NAME so the CloudFormation exports resolve."
  type        = string
  default     = "udacity-agentcore"
}

variable "frontend_origin" {
  description = "Origin used by the local browser frontend and Cognito callback URLs."
  type        = string
  default     = "http://localhost:8000"
}
