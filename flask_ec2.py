from flask import Flask
from flask import request
import json
import boto3
import logging
from botocore.exceptions import ClientError
import time
import os
import psycopg2

#Set the s3 credentials
aws_key_id = ''  # Change here, set your s3 access key id
aws_secret_key = ''  # Change here, set your s3 secret access key
region = 'us-east-1'
s3_bucket = "" # provide s3 bucket name
#Set the rds credentials
rds_host = '***************'
name = 'postgres'
password = '**************'
db_name = 'postgres'

app = Flask(__name__)



def upload_file(output_file, bucket, folder, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = output_file

    s3_client = boto3.client('s3',
         aws_access_key_id=''.join(aws_key_id),  # your s3 access key id
         aws_secret_access_key=''.join(aws_secret_key),  # your s3 secret access key
         )    
    
    #s3_client.put_object(Bucket= bucket, Key=folder)
    # Upload the file
    #s3_client = boto3.client('s3')
    try:
        #response = s3_client.upload_file(folder + output_file, bucket, 'folder/{}'.format(output_file))
        response = s3_client.upload_file(output_file,bucket,'%s/%s' %(folder, output_file))
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_output(tweet_list, output_file):
    json_file = open(output_file, 'w')
    tweet_dict = {}
    i = 1
    for x in tweet_list:
        #print(x)
        id_num = x[0]
        sentiment = x[1]
        score = x[2]

        tweet_dict[str(i)] = {}
        data = {}
        data['id'] = id_num
        data['sentiment'] = sentiment
        data['score'] = score

        tweet_dict[str(i)].update(data)

        i += 1

    result = json.dumps(tweet_dict)
    #print(result)
    json.dump(tweet_dict, json_file)
    json_file.close()
    return

@app.route("/sentiment/proxy/", methods=['POST','PUT'])
def post_proxy():
    if(request.is_json):
        start = time.time()
        bucket = s3_bucket #input("Enter the s3 bucket name:\n")
        output_filename = "output.json"#input("Enter the s3 output file name:\n")   

        content = request.get_json()
        input_filename = 'input.json'

        f = open(input_filename, 'w')
        json.dump(content, f)
        f.close()
        
        content_json = json.dumps(content)


        coding = str(int(time.time()))
        input_folder = 'input' + coding
        output_folder = 'output' + coding
        print("Your folder code:", coding)
        upload_file(input_filename, bucket, input_folder)

        start 
        client = boto3.client('lambda',
                        region_name=region,
                        aws_access_key_id=aws_key_id,
                        aws_secret_access_key=aws_secret_key)
        pattern = "proxy"

        payload = {"function":  pattern, "tweets": content_json}

        #payload = json.JSONEncoder().encode(payload)
        
        payload = json.dumps(payload)
        payload = json.loads(payload)
        #print(json.dumps(payload))

        result = client.invoke(FunctionName='lambda_handler1',
                    InvocationType='RequestResponse',                                      
                    Payload=json.dumps(payload))


        connection = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name, connect_timeout=5)
        cursor = connection.cursor()

        q1 = " SELECT id, sentiment, score FROM proxy_even.table1"
        cursor.execute(q1)
        res1 = cursor.fetchall()
        q2 = " SELECT id, sentiment, score FROM proxy_odd.table1"
        cursor.execute(q2)
        res2 = cursor.fetchall()
        res = res1 + res2

        create_output(res, output_filename)

        upload_file(output_filename, bucket, output_folder, object_name=None)
        print("Time taken to run: {0:.2f} seconds".format(time.time() - start) )

        return "Look for output.json file in output" + coding + " folder"
    else:
        return 'Input file must be json!'

@app.route("/sentiment/sharding/", methods=['POST','PUT'])
def post_sharding():
    if(request.is_json):
        start = time.time()
        bucket = s3_bucket #input("Enter the s3 bucket name:\n")
        output_filename = 'output.json' #input("Enter the s3 output file name:\n")
        
        content = request.get_json()

        input_filename = 'input.json'

        f = open(input_filename, 'w')
        json.dump(content, f)
        f.close()
        content_json = json.dumps(content)

        coding = str(int(time.time()))
        input_folder = 'input' + coding
        output_folder = 'output' + coding
        print("Your folder code:", coding)

        upload_file(input_filename, bucket, input_folder)
        #download_file(bucket, input_filename, folder)

        client = boto3.client('lambda',
                        region_name=region,
                        aws_access_key_id=aws_key_id,
                        aws_secret_access_key=aws_secret_key)
        pattern = "sharding"

        payload = {"function":  pattern, "tweets": content_json}

        payload = json.dumps(payload)
        payload = json.loads(payload)
        #print(json.dumps(payload))

        result = client.invoke(FunctionName='lambda_handler1',
                    InvocationType='RequestResponse',                                      
                    Payload=json.dumps(payload))

        connection = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name, connect_timeout=5)
        cursor = connection.cursor()

        q1 = " SELECT id, sentiment, score FROM shard_a2020.table1"
        cursor.execute(q1)
        res1 = cursor.fetchall()
        q2 = " SELECT id, sentiment, score FROM shard_b2020.table1"
        cursor.execute(q2)
        res2 = cursor.fetchall()
        res = res1 + res2
        #res = json.dumps(res)
        #print(res)
        create_output(res, output_filename)

        upload_file(output_filename, bucket, output_folder, object_name=None)
        print("Time taken to run: {0:.2f} seconds".format(time.time() - start) )

        return "Look for output.json file in output" + coding + " folder\n"
    else:
        return 'Input file must be json!'


if __name__ == '__main__':


    app.run(host='0.0.0.0', port=5000)

