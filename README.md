# DynamoDB to S3 Data Lake CDC Pipeline with Iceberg Tables

This repository implements a CDC (Change Data Capture) pipeline, transferring updates from AWS DynamoDB to an S3-based data lake in real-time. By leveraging AWS Lambda for processing DynamoDB Streams and utilizing Apache Iceberg tables within the AWS Glue Data Catalog, this solution ensures high fidelity and timeliness of data in your data lake.

## Features

- **Real-Time Synchronization**: Captures and processes DynamoDB table changes (inserts, updates, deletes) via DynamoDB Streams.
- **Lambda Processing**: Handles stream events to accurately reflect changes in the S3 data lake catalog.
- **Apache Iceberg Integration**: Utilizes Iceberg tables for supporting advanced operations like updates and deletes, overcoming the limitations of standard Hive-based tables.

## Why Apache Iceberg?

Apache Iceberg is chosen for its robust support for complex data operations not supported by traditional catalog tables:

- **ACID Transactions**: Guarantees atomicity, consistency, isolation, and durability for data operations.
- **Scalability**: Designed for efficient scaling, handling large datasets with ease.
- **Time Travel**: Facilitates accessing historical data snapshots, enabling data audit and rollback capabilities.

## Architecture Overview

1. **DynamoDB**: Source tables with enabled Streams for capturing data changes.
2. **AWS Lambda**: Processes stream events, implementing logic for CRUD operations in the data lake.
3. **Amazon S3 & AWS Glue Data Catalog with Iceberg**: Stores and manages the data lake in S3, using Iceberg tables for enhanced data operation capabilities.

## Getting Started

### Prerequisites

- AWS account with permissions for DynamoDB, Lambda, S3, and Glue.
- AWS CLI and SAM CLI installed and configured.

### Deployment Steps

1. **Enable DynamoDB Streams** on your DynamoDB table.
2. **Deploy the Lambda Function**:
   ```bash
   sam build
   sam deploy --guided
