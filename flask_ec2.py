from flask import Flask
from flask import request
import csv, json
import boto3
import logging
from botocore.exceptions import ClientError

#Set the s3 credentials
aws_key_id = 'AKIA5DSZSCPWLHXNPDWO'  # Change here, set your s3 access key id
aws_secret_key = 'GhnlpU26cdZMQbd6fd/6wQj5YbGeLW/ynHk7qofE'  # Change here, set your s3 secret access key

#Set the bucket and input file name
#s3_bucket = '' #Change here, set your bucket name
#input_name = '' #Change here. Set your csv input file name that will be predicted (file needs to be hosted on s3 bucket)
#output_name = '' #Change here. Set your csv output file for results after predicting


app = Flask(__name__)


def get_input():
    name_list = []
    bucket = input("Enter the s3 bucket name:\n")
    input_filename = input("Enter the s3 input file name:\n")
    output_filename = input("Enter the s3 output file name:\n")
    
    name_list.append(bucket)
    name_list.append(input_filename)
    name_list.append(output_filename)
    
    return name_list


def csv_to_json(names):

    s3_client = boto3.client('s3',
        aws_access_key_id=''.join(aws_key_id), #your s3 access key id
        aws_secret_access_key=''.join(aws_secret_key), #your s3 secret access key
        )   

    s3_client.download_file(names[0], names[1], names[1])  # downoloading file from S3

    csv_file2 = open(names[1], 'r', encoding = 'unicode_escape')
    json_file = open(names[2], 'w')

    reader2 = csv.DictReader(csv_file2)
    content = list(reader2)
    size = len(content)
    csv_file2.close()

    print(size)
    csv_file = open(names[1], 'r', encoding = 'unicode_escape')
    reader = csv.DictReader(csv_file)
    json_file.write("{")
    i = 0
    m = 1
    for row in reader:
        json_file.write('"' + str(m) + '":')
        json.dump(row, json_file)
        json_file.write("\n")
        if (i != size-1):
            json_file.write(",")
        i += 1
        m += 1

    json_file.write("}")
    csv_file.close()
    json_file.close()
    return 

def upload_file(names, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = names[2]

    s3_client = boto3.client('s3',
         aws_access_key_id=''.join(aws_key_id),  # your s3 access key id
         aws_secret_access_key=''.join(aws_secret_key),  # your s3 secret access key
         )    

    # Upload the file
    #s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(names[2], names[0], object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == '__main__':
    names = get_input()
    print(names)
    csv_to_json(names)
    upload_file(names)

    app.run(host='localhost', port=5000)
  

#@app.route('/sentiment/?data=' + file, methods = ['POST'])
#def calculate_heartrate_range():
 #   result = client.invoke(FunctionName=conf.lambda_function_name,
  #                  InvocationType='RequestResponse',                                      
   #                 Payload=json.dumps(payload))
    #range = result['Payload'].read()      
    #api_response = json.loads(range)               
    #return jsonify(api_response)












