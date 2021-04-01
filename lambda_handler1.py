import json
from textblob import TextBlob
import re
import nltk
from nltk.tokenize import WordPunctTokenizer
from nltk.sentiment import SentimentIntensityAnalyzer
import boto3

#Set the s3 credentials
aws_key_id = ''  # Change here, set your s3 access key id
aws_secret_key = ''  # Change here, set your s3 secret access key

tok = WordPunctTokenizer()

class Tweet:
    ''' 
    Generic Twitter Class for sentiment analysis. 
    '''

    def __init__(self):
        self.text = ""
        self.pat1 = r'@[A-Za-z0-9]+'
        self.pat2 = r'https?://[A-Za-z0-9./]+'
        self.combined_pat = r'|'.join((self.pat1, self.pat2))

    def set_text(self, text):
        self.text = text
        self.cln_text = self.tweet_cleaner()


    def get_sentiment_value(self): 
        ''' 
        Utility function to classify sentiment of passed tweet 
        using textblob's sentiment method 
        '''
        # create TextBlob object of passed tweet text 
        analysis = TextBlob(self.cln_text)  # 
        # set sentiment 
        score = analysis.sentiment.polarity

        return score


    def get_sentiment_string(self): 
        analyzer = SentimentIntensityAnalyzer()
        # set sentiment 
        if analyse.sentiment.polarity >= 0: 
            return 'positive'
        else: 
            return 'negative'


    def tweet_cleaner(self):

        stripped = re.sub(self.combined_pat, '', self.text)
        try:
            clean = stripped.decode("utf-8-sig").replace(u"\ufffd", "?")
        except:
            clean = stripped
        letters_only = re.sub("[^a-zA-Z]", " ", clean)
        lower_case = letters_only.lower()
        words = tok.tokenize(lower_case)
        results = (" ".join(words)).strip()
        return results


def lambda_handler(event, context):

    i = 1
    current_tweet = Tweet()
    tweet_dict = {}
    tweets = json.loads(event['tweets'])
    pattern = event['function']
    for x in tweets:
        date = tweets.get(str(i))['date']
        text = tweets.get(str(i))['text']
        id_num = tweets.get(str(i))['id']
        id_clean = re.sub(r'(0000\b)', "", id_num)

        current_tweet.set_text(text)

        score = current_tweet.get_sentiment_value()
        if score >= 0: 
            string = 'positive'
        else: 
            string = 'negative'

        score = abs(score)
           
        tweet_dict[str(i)] = {}
        data = {}
        data['date'] = date
        data['text'] = text
        data['id'] = id_clean
        data['sentiment'] = string
        data['score'] = score

        tweet_dict[str(i)].update(data)


        i += 1


    result = json.dumps(tweet_dict)
    payload = {"function":  pattern, "tweets": result}


    client = boto3.client('lambda',
                        aws_access_key_id=aws_key_id,
                        aws_secret_access_key=aws_secret_key)

    result = client.invoke(FunctionName='lambda_handler2',
                    InvocationType='RequestResponse',                                      
                    Payload=json.dumps(payload))
    return 0

        

    

