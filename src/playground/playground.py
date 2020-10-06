from crusher import twitter as tw
from live_feed.twitter_listener import listen_for_teams

listen_for_teams([tw.EvertonFC, tw.BrightonFC, tw.QPR, tw.SheffieldWednesday])
