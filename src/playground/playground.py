from crusher import twitter as tw
from live_feed.twitter_stream import listen_for_teams

listen_for_teams([tw.FiorentinaACF,
                  tw.SampdoriaUC,
                  tw.ParisStGermain,
                  tw.AngersSCO,
                  tw.BournemouthAFC,
                  tw.CoventryCity])
