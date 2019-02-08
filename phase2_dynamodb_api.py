''' New Brunswick Group - Recommendation System Project - 02/08/2019

Task: This Lambda function queries the the data associated with the event_id requested from API Gateway.

Script was edited based on Mr.Young's example.'''

import json
import boto3

DYNAMODB_TABLE = 'all_events_data2'

def lambda_handler(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE)
    

    try:
        #get event_id
        event_id = event['pathParameters']['id']
        print("event_id: {}".format(event_id))
        
        # Grab data based on the event_id
        response = table.get_item(Key={
            'event_id':event_id
        })
        
        
        # Edit the list of recommended event_id in the form of a Python list from a string.
        if response['Item']['recommendations'] != "[none']":
            recs_json = json.dumps(response['Item']['recommendations'])
            x = (recs_json.split(','))
            x[0] = x[0] .replace('[','')
            x[-1] = x[-1] .replace(']','')
            for i in range(len(x)):
                x[i] = x[i].replace('"','')
                x[i] = x[i].strip()
                x[i] = x[i].replace("'",'')
        
        response['Item']['recommendations'] = x
        
        return {
            "statusCode": 200, 
            "headers": {"Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS"
            },
            "body": json.dumps(response['Item']) # Output event_id data.
        }

    except:
        return {
            "statusCode": 404
        }