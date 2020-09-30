from crusher import twitter as tw
from live_feed.twitter_stream import listen_for_teams

listen_for_teams([tw.BurnleyFC, tw.ManchesterUtd, tw.ManchesterCity, tw.BrightonFC, tw.EvertonFC, tw.WestHamUtd])
