from flask import Flask
from flask import request
import csv, json


  
app = Flask(__name__)


def get_input():
	value = input("Do you want to use S3 or a local file? (type s3 or local):\n")
	if(value == 'local'):
		filename = input("Enter the file name? (file should be in the same directory with the script):\n")
	else:
		filename = ""
	return filename


def csv_to_json(csv_file):
    print("hi")
    csv_file2 = open(csv_file, 'r', encoding = 'unicode_escape')
    json_file = open('input.json', 'w')

    reader2 = csv.DictReader(csv_file2)
    content = list(reader2)
    size = len(content)
    csv_file2.close()

    print(size)
    csv_file = open(csv_file, 'r', encoding = 'unicode_escape')
    reader = csv.DictReader(csv_file)
    json_file.write("[")
    i = 0
    for row in reader:
        json.dump(row, json_file)
        json_file.write("\n")
        if (i != size-1):
            json_file.write(",")
        i += 1
    	#print(row)

    json_file.write("]")
    csv_file.close()
    json_file.close()
    return 




if __name__ == '__main__':
    file = get_input()
    print(file)
    csv_to_json(file)


    

    app.run(host='localhost', port=5000)
  

@app.route('/sentiment/?data=' + file, methods = ['POST'])
def calculate_heartrate_range():
    result = client.invoke(FunctionName=conf.lambda_function_name,
                    InvocationType='RequestResponse',                                      
                    Payload=json.dumps(payload))
    range = result['Payload'].read()      
    api_response = json.loads(range)               
    return jsonify(api_response)







