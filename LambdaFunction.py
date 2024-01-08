from __future__ import print_function
import json
import base64
import boto3
from decimal import Decimal
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('StockData_DB')
    for record in event['Records']:
        logger.info(record)
        payload=base64.b64decode(record['kinesis']["data"])
        logger.info("Decoded payload: " + str(payload))
        
        my_json = payload.decode('utf8').replace("'", '"')
        data = json.loads(my_json, parse_float=Decimal)
        
        if float(data['Adj Close']) >= (float(data['fiftyTwoWeekHigh'])*0.8) or float(data['Adj Close']) <= (float(data['fiftyTwoWeekLow'])*1.2):
            sns = boto3.client("sns")
            response = sns.publish(
                TopicArn='arn:aws:sns:us-east-1:637737936215:StockData',
                Message= 'Alert Generated for: ' + str(payload), #json.dumps({'default': json.dumps(data, use_decimal=True)}),
                Subject='SNS Notification of StockData',
                MessageStructure='string',
            )
            logger.info(response)
        response = table.put_item(
    		Item=data
    	)
        logger.info(response)