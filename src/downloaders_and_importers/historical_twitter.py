import GetOldTweets3 as got


def get_premier_league_tweets():
    criteria = got.manager.TweetCriteria().setUsername('@LFC').\
        setSince('2020-1-1').\
        setUntil('2020-3-1')

    tweets = got.manager.TweetManager.getTweets(criteria)
    tweets = [tweet for tweet in tweets if 'line-up' in tweet.text]
    return tweets


tweets = get_premier_league_tweets()
print(tweets)
