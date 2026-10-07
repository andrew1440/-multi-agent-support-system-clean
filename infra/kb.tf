# Task 5 - the three Bedrock Knowledge Bases.
#
# The classroom says to click these through the AWS Console. They are built here
# instead so teardown is one command and nothing is left behind. Every value is
# fixed by the rubric: the three names, the three S3 prefixes, the Titan v2
# embedding model, and the stack's S3 Vectors bucket with its matching index.
#
# This is working scope, not a deliverable. It is not part of the submission.

data "aws_caller_identity" "current" {}

# Resource names come from the CloudFormation stack the project deploys, so they
# are never duplicated here.
data "aws_cloudformation_export" "policy_bucket" {
  name = "${var.project_name}-PolicyBucket"
}

data "aws_cloudformation_export" "agentcore_role_arn" {
  name = "${var.project_name}-AgentCoreRoleArn"
}

locals {
  # Rubric: Returns -> policies/returns/, Shipping -> policies/shipping/,
  # Warranty -> policies/warranty/, each on its own matching vector index.
  policy_domains = {
    returns = {
      kb_name    = "novamart-returns-policy-kb"
      prefix     = "policies/returns/"
      index_name = "returns-policy-index"
    }
    shipping = {
      kb_name    = "novamart-shipping-policy-kb"
      prefix     = "policies/shipping/"
      index_name = "shipping-policy-index"
    }
    warranty = {
      kb_name    = "novamart-warranty-policy-kb"
      prefix     = "policies/warranty/"
      index_name = "warranty-policy-index"
    }
  }

  # Rubric: amazon.titan-embed-text-v2:0
  embedding_model_arn = "arn:aws:bedrock:${var.region}::foundation-model/amazon.titan-embed-text-v2:0"
  vector_bucket_arn   = aws_s3vectors_vector_bucket.policy.vector_bucket_arn
}

resource "aws_bedrockagent_knowledge_base" "policy" {
  for_each = local.policy_domains

  name        = each.value.kb_name
  description = "NovaMart ${each.key} policy knowledge base for the multi-agent support system."
  role_arn    = data.aws_cloudformation_export.agentcore_role_arn.value

  knowledge_base_configuration {
    type = "VECTOR"

    vector_knowledge_base_configuration {
      embedding_model_arn = local.embedding_model_arn
    }
  }

  # Self-managed on S3 Vectors, per the rubric. Not a Managed KB: that would
  # create a different vector bucket and the backing store cannot be changed after.
  storage_configuration {
    type = "S3_VECTORS"

    s3_vectors_configuration {
      vector_bucket_arn = aws_s3vectors_vector_bucket.policy.vector_bucket_arn
      index_name        = aws_s3vectors_index.policy[each.key].index_name
    }
  }
}

resource "aws_bedrockagent_data_source" "policy" {
  for_each = local.policy_domains

  knowledge_base_id = aws_bedrockagent_knowledge_base.policy[each.key].id
  name              = "${each.value.kb_name}-source"
  description       = "S3 prefix ${each.value.prefix} in the project policy bucket."

  data_source_configuration {
    type = "S3"

    s3_configuration {
      bucket_arn         = "arn:aws:s3:::${data.aws_cloudformation_export.policy_bucket.value}"
      inclusion_prefixes = [each.value.prefix]
    }
  }
}

output "kb_ids" {
  description = "Paste these into .env as RETURNS_KB_ID, SHIPPING_KB_ID, WARRANTY_KB_ID."
  value       = { for k, v in aws_bedrockagent_knowledge_base.policy : k => v.id }
}

output "data_source_ids" {
  description = "Needed to start an ingestion job (the console 'Sync' button) per data source."
  value       = { for k, v in aws_bedrockagent_data_source.policy : k => v.data_source_id }
}
