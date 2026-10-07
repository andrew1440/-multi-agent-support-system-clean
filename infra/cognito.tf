resource "aws_cognito_user_pool" "novamart" {
  name                     = "${var.project_name}-customers"
  username_attributes     = ["email"]
  auto_verified_attributes = ["email"]

  password_policy {
    minimum_length                   = 12
    require_lowercase                = true
    require_numbers                  = true
    require_symbols                  = false
    require_uppercase                = true
    temporary_password_validity_days = 7
  }

  schema {
    name                = "customer_id"
    attribute_data_type = "String"
    mutable             = true
    required            = false

    string_attribute_constraints {
      min_length = "1"
      max_length = "64"
    }
  }

  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  tags = {
    Project = var.project_name
    Purpose = "NovaMart customer authentication"
  }
}

resource "aws_cognito_user_pool_client" "web" {
  name         = "${var.project_name}-web"
  user_pool_id = aws_cognito_user_pool.novamart.id

  generate_secret                      = false
  prevent_user_existence_errors        = "ENABLED"
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["openid", "email", "profile"]
  supported_identity_providers         = ["COGNITO"]
  callback_urls                        = ["${var.frontend_origin}/"]
  logout_urls                          = ["${var.frontend_origin}/"]
  read_attributes                      = ["email", "email_verified", "custom:customer_id"]
  write_attributes                     = ["email", "custom:customer_id"]

  access_token_validity  = 60
  id_token_validity      = 60
  refresh_token_validity = 30
  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }
}

resource "aws_cognito_user_pool_domain" "novamart" {
  domain       = "${var.project_name}-${data.aws_caller_identity.current.account_id}"
  user_pool_id = aws_cognito_user_pool.novamart.id
}

output "cognito_user_pool_id" {
  value       = aws_cognito_user_pool.novamart.id
  description = "Set COGNITO_USER_POOL_ID in .env and web/config.js."
}

output "cognito_app_client_id" {
  value       = aws_cognito_user_pool_client.web.id
  description = "Set COGNITO_APP_CLIENT_ID in .env and web/config.js."
}

output "cognito_hosted_ui_domain" {
  value       = "${aws_cognito_user_pool_domain.novamart.domain}.auth.${var.region}.amazoncognito.com"
  description = "Hosted UI domain for browser login and logout."
}

output "cognito_oidc_discovery_url" {
  value       = "https://cognito-idp.${var.region}.amazonaws.com/${aws_cognito_user_pool.novamart.id}/.well-known/openid-configuration"
  description = "OIDC discovery URL used by the AgentCore CUSTOM_JWT authorizer."
}
