# DynamoDB tables which the lambda functions will use to trigger jobs and track state
resource "aws_dynamodb_table" "video_conversions" {
  name           = "FT_VideoConversions"
  read_capacity  = 20
  write_capacity = 20
  hash_key       = "ConversionID"

  attribute {
    name = "ConversionID"
    type = "S"
  }

  attribute {
    name = "QueueMessageID"
    type = "S"
  }

  attribute {
    name = "Created"
    type = "N"
  }

  attribute {
    name = "Updated"
    type = "N"
  }

  attribute {
    name = "Retries"
    type = "N"
  }

  attribute {
    name = "VideoURL"
    type = "S"
  }

  attribute {
    name = "RequestedFormats"
    type = "S"
  }

  tags {
    Name        = "VideoConversions"
    Environment = "production"
  }
}


resource "aws_dynamodb_table" "conversion_state" {
  name           = "FT_ConversionState"
  read_capacity  = 20
  write_capacity = 20
  hash_key       = "ConversionID"

  attribute {
    name = "ConversionID"
    type = "S"
  }

  attribute {
    name = "SegmentTotal"
    type = "N"
  }

  attribute {
    name = "SegmentsComplete"
    type = "N"
  }

  attribute {
    name = "ConcatReady"
    type = "N"
  }

  attribute {
    name = "Created"
    type = "N"
  }

  tags {
    Name        = "ConversionState"
    Environment = "production"
  }
}

resource "aws_dynamodb_table" "segment_state" {
  name           = "FT_SegmentState"
  read_capacity  = 20
  write_capacity = 20
  hash_key       = "SegmentID"


  attribute {
    name = "ConversionID"
    type = "S"
  }

  attribute {
    name = "SegmentID"
    type = "N"
  }

  attribute {
    name = "Completed"
    type = "N"
  }

  attribute {
    name = "Created"
    type = "N"
  }

  attribute {
    name = "ConversionFormat"
    type = "S"
  }

  tags {
    Name        = "SegmentState"
    Environment = "production"
  }
}

# S3 bucket which the lambda functions will interact with by default.
resource "aws_s3_bucket" "video_conversions" {
  bucket = "${var.unique_name}-FT_VideoConversions"
  acl    = "private"

  tags {
    Name        = "FTVideoConversions"
    Environment = "production"
  }
}

# IAM role for lambda access to S3, SQS, DynamoDB
resource "aws_iam_role" "iam_for_lambda" {
  name = "Fantastic_Transcoder_role"
}

resource "aws_iam_role_policy" "FT_s3_access" {
  name = "FT_s3_access"
  description = "Grants access to FT_VideoConversions s3 bucket"
  role = "${aws_iam_role.iam_for_lambda.arn}"
  policy = <<EOF
  {
    "Version": "2017-06-29",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:*",
        "Resource": [
          "${aws_s3_bucket.video_conversions.arn}",
          "${aws_s3_bucket.video_conversions.arn}/*"
        ],
      }
  }
EOF
}

resource "aws_iam_role_policy" "FT_sqs_access" {
  name = "FT_sqs_access"
  description = "Grants access to FT SQS queues"
  role = "${aws_iam_role.iam_for_lambda.arn}"
  policy = <<EOF
  {
    "Version": "2017-06-29",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": "*",
        "Action": "sqs:*",
        "Resource": [
          "${aws_sqs_queue.ft_videoconvert_queue.arn}",
          "${aws_sqs_queue.ft_status_queue.arn}",
          "${aws_sqs_queue.ft_deadletter_queue.arn}"
        ],
      }
  }
EOF
}

resource "aws_iam_role_policy" "FT_dynamodb_access" {
  name = "FT_dynamodb_access"
  description = "Grants access to FT DynamoDB tables"
  role = "${aws_iam_role.iam_for_lambda.arn}"
  policy = <<EOF
  {
    "Version": "2017-06-29",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": "*",
        "Action": "dynamodb:*",
        "Resource": [
          "${aws_dynamodb_table.FT_VideoConversions.arn}",
          "${aws_dynamodb_table.FT_SegmentState.arn}",
          "${aws_dynamodb_table.FT_ConversionState.arn}"
        ],
      }
  }
EOF
}

# SQS queue to initiate jobs
resource "aws_sqs_queue" "ft_videoconvert_queue" {
  name                      = "FT_convert_queue"
  delay_seconds             = 90
  max_message_size          = 2048
  message_retention_seconds = 1800
  receive_wait_time_seconds = 10
  redrive_policy            = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.ft_deadletter_queue.arn}\",\"maxReceiveCount\":4}"
}

# SQS deadletter queue
resource "aws_sqs_queue" "ft_deadletter_queue" {
  name                      = "FT_deadletter_queue"
  delay_seconds             = 90
  max_message_size          = 2048
  message_retention_seconds = 64000
  receive_wait_time_seconds = 10
}

# Status queue to track completion status
resource "aws_sqs_queue" "ft_status_queue" {
  name                      = "FT_status_queue"
  delay_seconds             = 90
  max_message_size          = 2048
  message_retention_seconds = 64000
  receive_wait_time_seconds = 10
}

# Cloudwatch alarm to alert on deadletter queue
resource "aws_cloudwatch_metric_alarm" "FT_deadletter_alarm" {
  alarm_name                = "FT_deadletter_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "QueueDepth"
  namespace                 = "AWS/SQS"
  period                    = "120"
  statistic                 = "Average"
  threshold                 = "1"
  alarm_description         = "This Alarm will alert you if Fantastic Transcoder fails to convert a video 5 consecutive times."
  dimensions {
    QueueName = "FT_deadletter_queue"
  }
}

# Lambda Functions
resource "aws_lambda_function" "ft_poll_lambda" {
  filename         = "ft_poll.zip"
  function_name    = "FT_poll"
  role             = "${aws_iam_role.iam_for_lambda.arn}"
  handler          = "poll.lambda_handler"
  source_code_hash = "${base64sha256(file("ft_segment.zip"))}"
  runtime          = "python2.7"
}

resource "aws_lambda_function" "ft_segment_lambda" {
  filename         = "ft_segment.zip"
  function_name    = "FT_segment"
  role             = "${aws_iam_role.iam_for_lambda.arn}"
  handler          = "segment.lambda_handler"
  source_code_hash = "${base64sha256(file("ft_segment.zip"))}"
  runtime          = "python2.7"
}

resource "aws_lambda_function" "ft_convert_lambda" {
  filename         = "ft_convert.zip"
  function_name    = "FT_convert"
  role             = "${aws_iam_role.iam_for_lambda.arn}"
  handler          = "convert.lambda_handler"
  source_code_hash = "${base64sha256(file("ft_convert.zip"))}"
  runtime          = "python2.7"
}

resource "aws_lambda_function" "ft_concat_lambda" {
  filename         = "ft_concat.zip"
  function_name    = "FT_concat"
  role             = "${aws_iam_role.iam_for_lambda.arn}"
  handler          = "concat.lambda_handler"
  source_code_hash = "${base64sha256(file("ft_concat.zip"))}"
  runtime          = "python2.7"
}