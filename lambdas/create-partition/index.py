import boto3
import os
import traceback

# Initialize Glue client
glue_client = boto3.client("glue")

def handler(event, context):
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = record["s3"]["object"]["key"]

        # Extract partition info from S3 key 
        table, year, month, day, hour, _ = source_key.split("/")

        # Form date for 'date' partition column
        date = "{}-{}-{}".format(year, month, day)

        # Form S3 location for partition
        s3_location = 's3://{}/{}/{}/{}/{}'.format(source_bucket, table, year, month, day, hour)

        # Form the partition input
        partition = {
            'Values': [
                date,
                hour
            ],
            "StorageDescriptor": {
                "NumberOfBuckets" : -1,
                "Columns": [
                    {
                        "Name": "timestamp",
                        "Type": "string"
                    },
                    {
                        "Name": "eventVersion",
                        "Type": "string"
                    },
                    {
                        "Name": "pipelineArn",
                        "Type": "string"
                    },
                    {
                        "Name": "pipeline",
                        "Type": "string"
                    },
                    {
                        "Name": "version",
                        "Type": "string"
                    },
                    {
                        "Name": "state",
                        "Type": "string"
                    },
                    {
                        "Name": "executionId",
                        "Type": "string"
                    }
                ],
                "Location": s3_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    }
                },
                "BucketColumns": [],
                "SortColumns": [],
                "Parameters": {},
                "SkewedInfo": {
                    "SkewedColumnNames": [],
                    "SkewedColumnValues": [],
                    "SkewedColumnValueLocationMaps": {}
                },
                "StoredAsSubDirectories": False
            }
        }

        # Add additional columns if the Glue table is for stage and action events
        if table == "stage-execution-state-change":
            partition["StorageDescriptor"]["Columns"].extend([
                {
                    "Name": "stage",
                    "Type": "string"
                }
            ])
        elif table == "action-execution-state-change":
            partition["StorageDescriptor"]["Columns"].extend([
                {
                    "Name": "stage",
                    "Type": "string"
                },
                {
                    "Name": "action",
                    "Type": "string"
                },
                {
                    "Name": "region",
                    "Type": "string"
                },
                {
                    "Name": "type",
                    "Type": "struct<owner:string,category:string,provider:string,version:string>"
                }
            ])

        # Attempt to create partition; Print exception if partition already exists or for any other error
        try:
            glue_client.create_partition(DatabaseName=os.environ["DATABASE_NAME"], TableName=table, PartitionInput=partition)
        except:
            traceback.print_exc()