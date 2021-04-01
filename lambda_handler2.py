import psycopg2
import json
import re

rds_host = 'postgres.cmf5w5wgnvbd.us-east-1.rds.amazonaws.com'
name = 'postgres'
password = 'fWTKbeC3EtgSMy6jLQzg'
db_name = 'postgres'


def lambda_handler(event, context):

    connection = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name, connect_timeout=5)
    cursor = connection.cursor()
    cursor.execute("truncate proxy_even.table1, proxy_odd.table1, shard_a2020.table1, shard_b2020.table1;")
    i = 1
    tweets = json.loads(event['tweets'])
    pattern = event['function']
    for x in tweets:
        date = tweets.get(str(i))['date']
        date_year = re.sub(r'(-\d\d.*)', "", date)
        text = tweets.get(str(i))['text']
        id_num = tweets.get(str(i))['id']
        sentiment = tweets.get(str(i))['sentiment']
        score = tweets.get(str(i))['score']
        if (pattern == 'proxy'):
            if (int(id_num) % 2) == 0:
                postgres_insert_query=""" INSERT INTO proxy_even.table1 (date, text, id, sentiment, score) VALUES (%s,%s,%s, %s, %s)"""
                record_to_insert=(date, text, id_num, sentiment, score)
                cursor.execute(postgres_insert_query, record_to_insert)
                connection.commit()
            else:
                postgres_insert_query=""" INSERT INTO proxy_odd.table1 (date, text, id, sentiment, score) VALUES (%s,%s,%s, %s, %s)"""
                record_to_insert=(date, text, id_num, sentiment, score)
                cursor.execute(postgres_insert_query, record_to_insert)
                connection.commit()
        elif (pattern == 'sharding'):
            if (date_year == '2020'):
                postgres_insert_query=""" INSERT INTO shard_a2020.table1 (date, text, id, sentiment, score) VALUES (%s,%s,%s, %s, %s)"""
                record_to_insert=(date, text, id_num, sentiment, score)
                cursor.execute(postgres_insert_query, record_to_insert)
                connection.commit()
            else:
                postgres_insert_query=""" INSERT INTO shard_b2020.table1 (date, text, id, sentiment, score) VALUES (%s,%s,%s, %s, %s)"""
                record_to_insert=(date, text, id_num, sentiment, score)
                cursor.execute(postgres_insert_query, record_to_insert)
                connection.commit()
        else:
            #print("Wrong pattern!")
            return 1
            
        i += 1
    return 0

