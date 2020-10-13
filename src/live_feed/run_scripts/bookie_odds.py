import json
import requests
from datetime import date

from live_feed.bookie_odds_collector import BookieOddsCollector
from live_feed.bookie_odds_collector import the_odds_api_league_code
from crusher.division import DivisionCodeEnum as DCEnum

api_key = 'C:\\Users\\rober\\crusher\\bookie_odds\\api_key.json'
with open(api_key, 'r') as f:
    api = json.load(f)


# --------Config-------- #
API_KEY = api['api_key']
SPORT = the_odds_api_league_code[DCEnum.LEAGUE_1]
REGION = 'uk'
MARKET = 'h2h'
SAVE_PATH = 'C:\\Users\\rober\\sport_data\\bookie_odds_prem_league.csv'
API_URL = 'https://api.the-odds-api.com/v3/sports'

# Run
sports_response = requests.get(API_URL, params={'api_key': API_KEY})
sports_json = json.loads(sports_response.text)
bookie_odds_collector = BookieOddsCollector(api_key=API_KEY,
                                            sport=SPORT,
                                            region=REGION,
                                            mkt=MARKET)

save_path = 'C:\\Users\\rober\\sport_data\\bookie_odds.csv'
bookie_odds_collector.stream_live_odds(log_odds=True, save_path=save_path, match_date_stream_filter=date.today())
