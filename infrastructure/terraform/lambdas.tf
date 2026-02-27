# ============================================
# LAMBDAS - DATA COLLECTOR Y ANALYTICS
# ============================================

# Lambda Data Collector
resource "aws_lambda_function" "data_collector" {
  filename         = "../../deployment/data_collector.zip"
  function_name    = "${var.project_name}-data-collector"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.handler"
  source_code_hash = filebase64sha256("../../deployment/data_collector.zip")
  runtime          = "python3.11"
  timeout          = 300
  memory_size      = 512

  vpc_config {
    subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
      EMPRESA       = "GSPSAS"
      BASE_DATOS    = "GSPSAS"
      API_TOKEN     = "0db03ce0e7f6ad6d153f7d53585fff6b"
    }
  }

  tags = {
    Name        = "${var.project_name}-data-collector"
    Environment = var.environment
  }
}

# Lambda Analytics Engine
resource "aws_lambda_function" "analytics_engine" {
  filename         = "../../deployment/analytics_engine.zip"
  function_name    = "${var.project_name}-analytics-engine"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.handler"
  source_code_hash = filebase64sha256("../../deployment/analytics_engine.zip")
  runtime          = "python3.11"
  timeout          = 300
  memory_size      = 512

  vpc_config {
    subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_group_ids = [ aws_security_group.lambda.id]
  }

  environment {
    variables = {
      DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
    }
  }

  tags = {
    Name        = "${var.project_name}-analytics-engine"
    Environment = var.environment
  }
}

# ============================================
# EVENTBRIDGE - PROGRAMACIÓN DIARIA
# ============================================

# Regla para Data Collector - 6 AM Colombia (11 AM UTC)
resource "aws_cloudwatch_event_rule" "data_collector_schedule" {
  name                = "${var.project_name}-data-collector-schedule"
  description         = "Ejecutar Data Collector diariamente a las 6 AM Colombia"
  schedule_expression = "cron(0 11 * * ? *)"

  tags = {
    Name        = "${var.project_name}-data-collector-schedule"
    Environment = var.environment
  }
}

resource "aws_cloudwatch_event_target" "data_collector_target" {
  rule      = aws_cloudwatch_event_rule.data_collector_schedule.name
  target_id = "data-collector"
  arn       = aws_lambda_function.data_collector.arn

  input = jsonencode({
    tipo = "ambos"
  })
}

resource "aws_lambda_permission" "allow_eventbridge_data_collector" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_collector.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.data_collector_schedule.arn
}

# Regla para Analytics Engine - 7 AM Colombia (12 PM UTC)
resource "aws_cloudwatch_event_rule" "analytics_schedule" {
  name                = "${var.project_name}-analytics-schedule"
  description         = "Ejecutar Analytics Engine diariamente a las 7 AM Colombia"
  schedule_expression = "cron(0 12 * * ? *)"

  tags = {
    Name        = "${var.project_name}-analytics-schedule"
    Environment = var.environment
  }
}

resource "aws_cloudwatch_event_target" "analytics_target" {
  rule      = aws_cloudwatch_event_rule.analytics_schedule.name
  target_id = "analytics-engine"
  arn       = aws_lambda_function.analytics_engine.arn
}

resource "aws_lambda_permission" "allow_eventbridge_analytics" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analytics_engine.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.analytics_schedule.arn
}

# ============================================
# OUTPUTS ADICIONALES
# ============================================

output "lambda_data_collector_arn" {
  value       = aws_lambda_function.data_collector.arn
  description = "ARN del Lambda Data Collector"
}

output "lambda_analytics_arn" {
  value       = aws_lambda_function.analytics_engine.arn
  description = "ARN del Lambda Analytics Engine"
}
