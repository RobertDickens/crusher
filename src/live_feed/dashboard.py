from multiprocessing import Process
from crusher import twitter as tw
from live_feed.twitter.twitter_listener import listen_for_teams
import json
import requests
from datetime import date

from live_feed.the_odds_api.bookie_odds_collector import BookieOddsCollector
from live_feed.the_odds_api.bookie_odds_collector import the_odds_api_league_code
from crusher.division import DivisionCodeEnum as DCEnum


def twitter_listener():
    listen_for_teams([tw.SheffieldUtd,
                      tw.FulhamFC,
                      tw.EvertonFC,
                      tw.LiverpoolFC,
                      tw.NewcastleUtd,
                      tw.ManchesterUtd,
                      tw.BournemouthAFC,
                      tw.QPR])


def bookie_odds_streamer():
    api_key = 'C:\\Users\\rober\\crusher\\bookie_odds\\api_key.json'
    with open(api_key, 'r') as f:
        api = json.load(f)

    # --------Config-------- #
    API_KEY = api['api_key']
    SPORT = the_odds_api_league_code[DCEnum.PREMIER_LEAGUE]
    REGION = 'uk'
    MARKET = 'h2h'

    # Run
    bookie_odds_collector = BookieOddsCollector(api_key=API_KEY,
                                                sport=SPORT,
                                                region=REGION,
                                                mkt=MARKET)

    save_path = 'C:\\Users\\rober\\sport_data\\bookie_odds_2020_10_17.csv'
    bookie_odds_collector.stream_live_odds(log_odds=False,
                                           save_path=save_path,
                                           match_date_stream_filter=date(2020, 10, 18),
                                           refresh_rate_seconds=300)


if __name__ == '__main__':
  p1 = Process(target=twitter_listener)
  p1.start()
  p2 = Process(target=bookie_odds_streamer)
  p2.start()
  p1.join()
  p2.join()
