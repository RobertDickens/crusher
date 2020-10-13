import tweepy
from live_feed.twitter.authentication import auth
import playsound


class TwitterListener(tweepy.StreamListener):
    def __init__(self, teams):
        super().__init__()
        self.teams = teams

    def on_status(self, tweet_data):
        if not self.filter_tweet(tweet_data):
            print(tweet_data.text)
            print(tweet_data.author.screen_name)
            playsound.playsound('C:\\Windows\\Media\\notify.wav')
            return True

    def on_error(self, status):
        if status == 420:
            # returning false on data method in case rate limit occurs
            return False
        print(status)

    def filter_tweet(self, tweet_data):
        is_reply = tweet_data.in_reply_to_status_id is not None
        is_retweeted = tweet_data.retweeted is True
        rt_in_text = 'RT ' in tweet_data.text
        at_in_text = any([team.handle.lower() in tweet_data.text.lower() for team in self.teams])
        return any([is_reply, is_retweeted, rt_in_text, at_in_text])


def listen_for_teams(teams):
    listener = TwitterListener(teams=teams)
    my_stream = tweepy.Stream(auth=auth, listener=listener)
    my_stream.filter(follow=[team.twitter_id for team in teams])
