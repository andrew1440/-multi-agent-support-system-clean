resource "aws_dynamodb_table" "agent_sessions" {
  name         = "agent-sessions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "session_id"
  range_key    = "record_key"

  attribute {
    name = "session_id"
    type = "S"
  }

  attribute {
    name = "record_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Project = var.project_name
    Purpose = "Strands persistent agent sessions"
  }
}

resource "aws_iam_role_policy" "agent_sessions" {
  name = "${var.project_name}-agent-sessions-access"
  role = element(
    split("/", data.aws_cloudformation_export.agentcore_role_arn.value),
    length(split("/", data.aws_cloudformation_export.agentcore_role_arn.value)) - 1,
  )

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:Query",
        "dynamodb:UpdateItem",
      ]
      Resource = [
        aws_dynamodb_table.agent_sessions.arn,
      ]
    }]
  })
}