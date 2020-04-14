# aws-codepipeline-event-logs
AWS CodePipeline event logs stored in S3 and query-able by Athena

## Architecture

![enter image description here](https://d50daux61fgb.cloudfront.net/aws-codepipeline-event-logs/solution-architecture.png)

Three EventBridge event rules for the default event bus track each type of CodePipeline event. Each event rule targets a specific Kinesis Firehose delivery stream that will put log files into a certain S3 folder corresponding to a Glue table. A Lambda function is used by each delivery stream to transform the CloudWatch event payload into the log file format. After a log file is placed in the bucket, a Lambda function is run that creates a partition for the corresponding Glue table, is one does not exist.

## Event Types

### Pipeline Execution State Change

The execution state of a pipeline has changed. 

#### Log Format

 - **timestamp** *(string)* - The ISO 8601 format of the timestamp of the event
 - **eventVersion** *(string)* - The version of the event format
 - **pipelineArn** *(string)* - The ARN of the pipeline
 - **pipeline** *(string)* - The name of the pipeline
 - **version** *(string)* - The version of the pipeline
 - **state** *(string)* - The pipeline state
 - **executionId** *(string)* - The pipeline execution ID


### Stage Execution State Change

The execution state of a pipeline stage has changed. 

#### Log Format

 - **timestamp** *(string)* - The ISO 8601 format of the timestamp of the event
 - **eventVersion** *(string)* - The version of the event format
 - **pipelineArn** *(string)* - The ARN of the pipeline
 - **pipeline** *(string)* - The name of the pipeline
 - **version** *(string)* - The version of the pipeline
 - **stage** *(string)* - The name of the pipeline stage
 - **state** *(string)* - The stage state
 - **executionId** *(string)* - The pipeline execution ID


 ### Action Execution State Change

The execution state of an action in a pipeline stage has changed. 

#### Log Format

 - **timestamp** *(string)* - The ISO 8601 format of the timestamp of the event
 - **eventVersion** *(string)* - The version of the event format
 - **pipelineArn** *(string)* - The ARN of the pipeline
 - **pipeline** *(string)* - The name of the pipeline
 - **version** *(string)* - The version of the pipeline
 - **stage** *(string)* - The name of the pipeline stage
 - **action** *(string)* - The name of the action
 - **state** *(string)* - The action state
 - **executionId** *(string)* - The pipeline execution ID
 - **type** *(object)* - Describes the action's type ID
    - **owner** *(string)* - 'AWS'|'ThirdParty'
    - **category** *(string)* - 'Source'|'Build'|'Test'|'Deploy'|'Approval'|'Invoke'
    - **provider** *(string)* - The service that provides the resources for performing the action
    - **version** *(string)* - The version of the action type