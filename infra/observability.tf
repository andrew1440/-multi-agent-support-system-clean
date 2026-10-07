locals {
  agent_metrics_namespace = "${var.project_name}/AgentSupport"
  agent_metric_names = [
    "OrchestratorAgent",
    "InventoryAgent",
    "RefundAgent",
    "PolicyAgent",
    "CommunicationAgent",
    "ReturnsPolicyRetrieverAgent",
    "ShippingPolicyRetrieverAgent",
    "WarrantyPolicyRetrieverAgent",
  ]
  agentcore_role_name = element(
    split("/", data.aws_cloudformation_export.agentcore_role_arn.value),
    length(split("/", data.aws_cloudformation_export.agentcore_role_arn.value)) - 1,
  )
}

resource "aws_iam_role_policy" "agent_sessions_and_metrics" {
  name = "${var.project_name}-sessions-and-metrics"
  role = local.agentcore_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
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
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = local.agent_metrics_namespace
          }
        }
      },
    ]
  })
}

resource "aws_cloudwatch_dashboard" "agent_support" {
  dashboard_name = "${var.project_name}-agent-support"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Agent invocations"
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Sum"
          metrics = [
            for agent in local.agent_metric_names : [
              local.agent_metrics_namespace,
              "AgentInvocationCount",
              "AgentName",
              agent,
            ]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Average response latency by agent (ms)"
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Average"
          metrics = [
            for agent in local.agent_metric_names : [
              local.agent_metrics_namespace,
              "AgentLatencyMilliseconds",
              "AgentName",
              agent,
            ]
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Guardrail interventions"
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Sum"
          metrics = [
            for agent in local.agent_metric_names : [
              local.agent_metrics_namespace,
              "GuardrailInterventionCount",
              "AgentName",
              agent,
            ]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Agent invocation failures"
          view    = "timeSeries"
          stacked = false
          region  = var.region
          period  = 300
          stat    = "Sum"
          metrics = [
            for agent in local.agent_metric_names : [
              local.agent_metrics_namespace,
              "AgentInvocationFailureCount",
              "AgentName",
              agent,
            ]
          ]
        }
      },
    ]
  })
}