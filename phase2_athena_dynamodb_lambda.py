''' New Brunswick Group - Recommendation System Project - 02/08/2019

Task: Grab clean event data from Athena and input it into a DynamoDB table using this Lambda function.

Script was edited based on Mr.Young's example.'''


from pprint import pprint
import time
import json
import boto3

# Define tables and s3 storage buckets
ATHENA_DATABASE = 'events-rec-system'
ATHENA_TABLE = 'dma_parquet'
DYNAMODB_TABLE = 'all_events_data2'
S3_OUTPUT = 's3://events-data-only/recommendations'
S3_BUCKET = 'events-data-only'
PAGE_SIZE = 500

# Number of retries.
RETRY_COUNT = 10

def lambda_handler(event, context):
    '''Query data from defined Athena table to new DynamoDB table.'''
    
    # This query will only take the 10 instances from the Athena table.
    query = "select * from {} limit 10;".format(ATHENA_TABLE)
    
    # Call Athena
    athena = boto3.client('athena')
    
    # Execute query and output into defined s3 output.
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': ATHENA_DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )
    
    # Save query_execution_id to make sure query succeeded.
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    for i in range(1, 1 + RETRY_COUNT):

        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        athena.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')
    
    # Call DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE)
    keys = None
    count = 0
    
    
    results_paginator = athena.get_paginator('get_query_results')
    results_iterator = results_paginator.paginate(
        QueryExecutionId=query_execution_id,
            PaginationConfig={
            'PageSize': PAGE_SIZE,
            'StartingToken': None
        }
    )
    
    # Insert queried Athena data to DynamoDB table.
    for result in results_iterator:
        
        if not keys:
            keys = [c['VarCharValue'].encode('ascii','ignore') for c in result['ResultSet']['Rows'][0]['Data']]
            result['ResultSet']['Rows'].pop(0)
        
        with table.batch_writer(overwrite_by_pkeys=['event_id', 'event_id']) as batch:
            for row in result['ResultSet']['Rows']:
                values = []
                for c in row['Data']:
                    if c.keys():
                        if c[u'VarCharValue'] == '':
                            values.append('unknown')
                        else:
                            values.append(c[u'VarCharValue'].encode('ascii','ignore'))
                        
                        print('yes')
                    else:
                        values.append('unknown')
                #values = [c[u'VarCharValue'].encode('ascii','ignore') for c in row['Data']]
                item = dict(zip(keys, values))
                print(item)
                batch.put_item(Item=item)
                #pprint("added {}".format(json.dumps(item)))
                count += 1
        
    return count