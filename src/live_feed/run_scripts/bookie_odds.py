import json
import requests

from live_feed.bookie_odds_collector import BookieOddsCollector


api_key = 'C:\\Users\\rober\\crusher\\bookie_odds\\api_key.json'
with open(api_key, 'r') as f:
    api = json.load(f)


# --------Config-------- #
API_KEY = api['api_key']
SPORT = 'soccer_epl'
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

bookie_odds_collector.log_odds_to_csv('C:\\Users\\rober\\sport_data\\bookie_odds_prem_league.csv')

