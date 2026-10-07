# S3 Vectors backing store for the three Knowledge Bases.
#
# The Environment Setup page says the CloudFormation stack creates "a vector
# bucket with three vector indexes". It does not: starter_stack.yaml declares
# VectorStoreBucket as a plain AWS::S3::Bucket and creates no indexes. A Bedrock
# Knowledge Base on S3 Vectors needs a real vector bucket and one index per KB,
# and the index must exist before the KB is created. Both are created here.
#
# Titan Embed Text v2 emits 1024-dimensional vectors, and Bedrock Knowledge Bases
# query S3 Vectors by cosine distance.

resource "aws_s3vectors_vector_bucket" "policy" {
  vector_bucket_name = "${var.project_name}-vectors-${data.aws_caller_identity.current.account_id}"
  force_destroy      = true
}

resource "aws_s3vectors_index" "policy" {
  for_each = local.policy_domains

  vector_bucket_name = aws_s3vectors_vector_bucket.policy.vector_bucket_name
  index_name         = each.value.index_name
  data_type          = "float32"
  dimension          = 1024
  distance_metric    = "cosine"
}
