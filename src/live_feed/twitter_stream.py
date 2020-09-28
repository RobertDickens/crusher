import tweepy
from live_feed.authentication import auth


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, blah):
        if blah.in_reply_to_status_id is None and blah.retweeted is False and 'RT ' not in blah.text and '@LFC' not in blah.text:
            print(blah.text)
            return True

    def on_error(self, status):
        if status == 420:
            # returning false on data method in case rate limit occurs
            return False
        print(status)


class TwitterListener:
    pass


listener = MyStreamListener()
myStream = tweepy.Stream(auth=auth, listener=listener)
myStream.filter(follow=["19583545"])
# TODO:
# Set up clean class, make configs for team twitter accounts and integrate into streaming class
