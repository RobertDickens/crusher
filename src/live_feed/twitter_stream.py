import tweepy
from live_feed.authentication import auth


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)
        return True

    def on_error(self, status):
        print(status)


listener = MyStreamListener()
myStream = tweepy.Stream(auth=auth, listener=listener)
myStream.filter(track=['liverpool'])

