import json
from textblob import TextBlob
import re 

class Tweet:
    ''' 
    Generic Twitter Class for sentiment analysis. 
    '''

    def __init__(self):
        self.text = ""



    def set_text(self, text):
        self.text = text

    def clean_tweet(self): 
        ''' 
        Utility function to clean tweet text by removing links, special characters 
        using simple regex statements. 
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", self.text).split())
    

    def get_sentiment_value(self, tweet): 
        ''' 
        Utility function to classify sentiment of passed tweet 
        using textblob's sentiment method 
        '''
        # create TextBlob object of passed tweet text 
        analysis = TextBlob(tweet)  # self.clean_tweet(tweet)
        # set sentiment 
        return analysis.sentiment.polarity

    def get_sentiment_string(self, tweet): 
        ''' 
        Utility function to classify sentiment of passed tweet 
        using textblob's sentiment method 
        '''
        # create TextBlob object of passed tweet text 
        analyse = TextBlob(tweet)  # self.clean_tweet(tweet)
        # set sentiment 
        if analyse.sentiment.polarity >= 0: 
            return 'positive'
        else: 
            return 'negative'







if __name__ == '__main__':


    with open('input.json', 'r') as f:
        file = json.load(f)

    current_tweet = Tweet()

    for tweet in file:
        text = tweet['text']
        print(text)
        current_tweet.set_text(text)
        text = current_tweet.clean_tweet()
        print(text)

        score = current_tweet.get_sentiment_value(text)
        string = current_tweet.get_sentiment_string(text)
        print(score)
        print(string)

