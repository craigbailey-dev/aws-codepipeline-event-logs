import json
import base64
import traceback
from datetime import datetime

def handler(event, context):
    output = []
    for record in event["records"]:
        try:
            entry = base64.b64decode(record["data"]).decode("utf-8")
            parsed_entry = json.loads(entry)
            payload = parsed_entry["detail"]
            payload["execution_id"] = payload["execution-id"]
            del payload["execution-id"]
            payload["pipelineArn"] = parsed_entry["resources"][0]
            payload["eventVersion"] = parsed_entry["version"]
            payload["timestamp"] = parsed_entry["time"]
            decoded_data = json.dumps(payload) + "\n"
            encoded_data = base64.b64encode(decoded_data.encode("utf-8")).decode("utf-8")
            output.append({
                "recordId": record["recordId"],
                "result": "Ok",
                "data": encoded_data,
            })
        except:
            traceback.print_exc()
            output.append({
                "recordId": record["recordId"],
                "result": "ProcessingFailed",
                "data": record["data"],
            })
    return {
        "records": output
    }