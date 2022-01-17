import boto3
from datetime import datetime
import calendar
import random
import time
import json

# Create a kinesis stream from the console and use the name here
stream_name = 'sample'

k_client = boto3.client('kinesis', region_name='us-east-1')

def lambda_handler(event, context):
    # Create Dummy records for pushing it to kinesis
    for i in range(100):
        id = random.randint(0, 100)
        start_date = calendar.timegm(datetime.utcnow().timetuple())
        data = 'sample' + str(id)
        
        # write the data to the stream
        put_to_stream(data, id, start_date)

        # wait for 1 second
        time.sleep(1)
        
def put_to_stream(data, value, date):
    
    payload = {
                'id': str(value),
                'start_date': str(date),
                'payload': data
              }

    print(payload)

    put_response = k_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=data)