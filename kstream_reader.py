import boto3
from datetime import datetime
import time

def lambda_handler(event, context):

    # The stream created in aws console
    my_stream_name = 'sample'

    kinesis_client = boto3.client('kinesis', region_name='us-east-1')

    response = kinesis_client.describe_stream(StreamName=my_stream_name)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    # We use ShardIteratorType of LATEST which means that we start to look
    # at the end of the stream for new incoming data. Note that this means
    # you should be running the this lambda before running any write lambdas

    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')

    # getting the shard number and set up iterator
    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,Limit=100)
 
    while 'NextShardIterator' in record_response:
        # read up to 100 records at a time from the shard number
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],Limit=100)
        # Print only if we have something
        if(record_response['Records']):
            print (record_response['Records'][0]['Data'].decode('utf-8'))

        # wait for 1 seconds before looping back around to see if there is any more data to read
        time.sleep(1)