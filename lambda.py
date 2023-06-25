# This is the first lambda function: for serializing image data
import json
import boto3
import base64

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """A function to serialize target data from S3"""

    # Get the s3 address from the Step Function event input
    key = event['s3_key']
    bucket = event['s3_bucket']

    # Download the data from s3 to /tmp/image.png
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).download_file('/tmp/image.png')

    # We read the data from a file
    with open("/tmp/image.png", "rb") as f:
        image_data = base64.b64encode(f.read())

    # Pass the data back to the Step Function
    print("Event:", event.keys())
    return {
        'statusCode': 200,
        'body': {
            "image_data": image_data,
            "s3_bucket": bucket,
            "s3_key": key,
            "inferences": []
        }
    }



# This is the second lambda function: for classifying image data
import os
import io
import boto3
import json
import base64

# grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime= boto3.client('runtime.sagemaker')

def lambda_handler(event, context):

    # Grab the image_data from the event
    # Use this while attaching this lambda into Step Function.

    image = event['Payload']['body']['image_data']

    image = base64.b64decode(image)

    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,ContentType='application/x-image',Body=image)
    print("Response:",response)

    result = json.loads(response['Body'].read().decode())
    print("result: ",result)

   # We return the data back to the Step Function
    event["inferences"] = result
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }

# This is the third lambda function: to filter low-confidence inferences
import json


THRESHOLD = 0.98


def lambda_handler(event, context):

    # Grab the inferences from the event
    body = event['Payload']['body']
    data = json.loads(body)
    inferences = data['inferences']

    # Grab the inferences from the event
    # Use this when testing this third lambda function alone
    # inferences = event['inferences']

    # Check if any values in our inferences are above THRESHOLD
    if max(inferences) > THRESHOLD:
        meets_threshold = True
    else:
        meets_threshold = False

    # If our threshold is met, pass our data back out of the
    # Step Function, else, end the Step Function with an error
    if meets_threshold:
        pass
    else:
        raise("THRESHOLD_CONFIDENCE_NOT_MET")

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
