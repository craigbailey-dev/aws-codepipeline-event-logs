import boto3
import os
import traceback

glue_client = boto3.client("glue")

def form_partition(table, date, hour, s3_location):
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
                    "Name": "execution_id",
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
    return partition


def handler(event, context):
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = record["s3"]["object"]["key"]
        table, year, month, day, hour, _ = source_key.split("/")
        date = "{}-{}-{}".format(year, month, day)
        s3_location = 's3://{}/{}/{}/{}/{}'.format(source_key, table, year, month, day, hour)
        partition = form_partition(table, date, hour, s3_location)
        try:
            glue_client.create_partition(DatabaseName=os.environ["DATABASE_NAME"], TableName=table, PartitionInput=partition)
        except:
            traceback.print_exc()