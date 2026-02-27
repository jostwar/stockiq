# ============================================
# PLATAFORMA DE GESTIÓN DE INVENTARIO
# Infraestructura AWS con Terraform
# ============================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ============================================
# VARIABLES
# ============================================

variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
  default     = "inventory-platform"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Región AWS"
  type        = string
  default     = "us-east-1"  # Cambiar según tu preferencia
}

variable "db_username" {
  description = "Usuario administrador de la base de datos"
  type        = string
  default     = "inventory_admin"
}

variable "db_password" {
  description = "Contraseña de la base de datos (usar secrets en producción)"
  type        = string
  sensitive   = true
}

variable "allowed_ip" {
  description = "Tu IP pública para acceso directo a RDS (desarrollo)"
  type        = string
  default     = "0.0.0.0/0"  # CAMBIAR por tu IP en producción
}

# ============================================
# PROVIDER
# ============================================

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# ============================================
# VPC Y NETWORKING
# ============================================

# VPC
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# Subnets públicas (para acceso desde internet en desarrollo)
resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-1"
  }
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-2"
  }
}

# Subnets privadas (para RDS en producción)
resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.10.0/24"
  availability_zone = "${var.aws_region}a"

  tags = {
    Name = "${var.project_name}-private-1"
  }
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.11.0/24"
  availability_zone = "${var.aws_region}b"

  tags = {
    Name = "${var.project_name}-private-2"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

# Route Table pública
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public_1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.public.id
}

# ============================================
# SECURITY GROUPS
# ============================================

# Security Group para RDS
resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Security group para RDS PostgreSQL"
  vpc_id      = aws_vpc.main.id

  # Acceso PostgreSQL desde Lambda
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
    description     = "PostgreSQL desde Lambda"
  }

  # Acceso PostgreSQL desde tu IP (desarrollo)
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ip]
    description = "PostgreSQL desde IP desarrollo"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-rds-sg"
  }
}

# Security Group para Lambda
resource "aws_security_group" "lambda" {
  name        = "${var.project_name}-lambda-sg"
  description = "Security group para Lambda functions"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-lambda-sg"
  }
}

# ============================================
# RDS POSTGRESQL
# ============================================

# Subnet Group para RDS
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = [aws_subnet.public_1.id, aws_subnet.public_2.id]  # Públicas para desarrollo

  tags = {
    Name = "${var.project_name}-db-subnet-group"
  }
}

# RDS PostgreSQL Instance
resource "aws_db_instance" "main" {
  identifier     = "${var.project_name}-db"
  engine         = "postgres"
  engine_version = "15"
  
  # Tamaño (ajustar según necesidad)
  instance_class        = "db.t3.micro"  # Eligible para free tier
  allocated_storage     = 20
  max_allocated_storage = 100  # Auto-scaling hasta 100GB
  storage_type          = "gp3"
  
  # Credenciales
  db_name  = "inventory_db"
  username = var.db_username
  password = var.db_password
  
  # Networking
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true  # TRUE para desarrollo, FALSE para producción
  
  # Backup y mantenimiento
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "Mon:04:00-Mon:05:00"
  
  # Otras configuraciones
  multi_az               = false  # TRUE para producción
  skip_final_snapshot    = true   # FALSE para producción
  deletion_protection    = false  # TRUE para producción
  
  # Performance Insights (gratis en t3.micro)
  performance_insights_enabled = true
  
  tags = {
    Name = "${var.project_name}-db"
  }
}

# ============================================
# SECRETS MANAGER (para credenciales)
# ============================================

resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "${var.project_name}/db-credentials"
  description = "Credenciales de la base de datos"
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = var.db_username
    password = var.db_password
    host     = aws_db_instance.main.address
    port     = 5432
    dbname   = "inventory_db"
  })
}

# ============================================
# S3 BUCKET (para frontend y reportes)
# ============================================

resource "aws_s3_bucket" "frontend" {
  bucket = "${var.project_name}-frontend-${var.environment}"
}

resource "aws_s3_bucket_website_configuration" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}

resource "aws_s3_bucket_public_access_block" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# ============================================
# SNS TOPIC (para alertas)
# ============================================

resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-alerts"
}

# Suscripción por email (agregar tu email)
# resource "aws_sns_topic_subscription" "email" {
#   topic_arn = aws_sns_topic.alerts.arn
#   protocol  = "email"
#   endpoint  = "tu-email@ejemplo.com"
# }

# ============================================
# IAM ROLE PARA LAMBDA
# ============================================

resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Políticas para Lambda
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.db_credentials.arn
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface"
        ]
        Resource = "*"
      }
    ]
  })
}

# ============================================
# OUTPUTS
# ============================================

output "rds_endpoint" {
  description = "Endpoint de conexión a RDS"
  value       = aws_db_instance.main.address
}

output "rds_port" {
  description = "Puerto de RDS"
  value       = aws_db_instance.main.port
}

output "database_name" {
  description = "Nombre de la base de datos"
  value       = "inventory_db"
}

output "secrets_manager_arn" {
  description = "ARN del secreto con credenciales"
  value       = aws_secretsmanager_secret.db_credentials.arn
}

output "sns_topic_arn" {
  description = "ARN del topic SNS para alertas"
  value       = aws_sns_topic.alerts.arn
}

output "s3_bucket_name" {
  description = "Nombre del bucket S3 para frontend"
  value       = aws_s3_bucket.frontend.id
}

output "lambda_role_arn" {
  description = "ARN del rol IAM para Lambda"
  value       = aws_iam_role.lambda.arn
}

output "lambda_security_group_id" {
  description = "ID del Security Group para Lambda"
  value       = aws_security_group.lambda.id
}

output "vpc_id" {
  description = "ID de la VPC"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "IDs de subnets privadas"
  value       = [aws_subnet.private_1.id, aws_subnet.private_2.id]
}

output "connection_string" {
  description = "String de conexión a PostgreSQL (sin password)"
  value       = "postgresql://${var.db_username}:****@${aws_db_instance.main.address}:5432/inventory_db"
  sensitive   = false
}
# VPC Endpoint para Secrets Manager
resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.lambda.id]
  private_dns_enabled = true

  tags = {
    Name = "${var.project_name}-secretsmanager-endpoint"
  }
}