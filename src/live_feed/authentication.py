import tweepy
import json

with open('C:\\Users\\rober\\crusher\\twitter\\credentials.json', 'r') as fp:
    d = json.load(fp)

auth = tweepy.OAuthHandler(d['consumer_key'], d['consumer_secret'])
auth.set_access_token(d['access_token'], d['access_token_secret'])
