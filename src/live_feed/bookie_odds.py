import json
import requests
import datetime
import pandas as pd
import os
import time

# An api key is emailed to you when you sign up to a plan
api_key = 'C:\\Users\\rober\\crusher\\bookie_odds\\api_key.json'
with open(api_key, 'r') as f:
    api = json.load(f)

API_KEY = api['api_key']
SPORT = 'soccer_epl'
REGION = 'uk'
MARKET = 'h2h'

sports_response = requests.get('https://api.the-odds-api.com/v3/sports', params={
    'api_key': API_KEY
})

sports_json = json.loads(sports_response.text)


class BookieOddsCollector:
    def __init__(self, api_key, sport, region, mkt):
        self.api_key = api_key
        self.sport = sport
        self.region = region
        self.mkt = mkt

    def get_results(self):
        odds_response = requests.get('https://api.the-odds-api.com/v3/odds',
                                     params={'api_key': self.api_key,
                                             'sport': self.sport,
                                             'region': self.region,
                                             'mkt': self.mkt})
        odds_json = json.loads(odds_response.text)
        print('Remaining requests', odds_response.headers['x-requests-remaining'])
        print('Used requests', odds_response.headers['x-requests-used'])

        if not odds_json['success']:
            raise ValueError(odds_json['msg'])

        odds_data = odds_json['data']
        return self._parse_data_to_df(odds_data)

    def log_odds_to_csv(self, csv_path, refresh_rate_minutes=None):
        starttime = time.time()
        while True:
            df = self.get_results()
            if not os.path.exists(csv_path):
                df.to_csv('my_csv.csv', mode='w', header=True)

            print("Logged odds")
            time.sleep(60.0 - ((time.time() - starttime) % 60.0))

    def _parse_data_to_df(self, odds_data):
        rows = {'site': [],
                'home_team': [],
                'away_team': [],
                'home_team_odds': [],
                'away_team_odds': [],
                'draw_odds': [],
                'update_datetime': []}
        for event in odds_data:
            home_team = event['teams'][0].lower()
            away_team = event['teams'][1].lower()
            for site in event['sites']:
                rows['site'].append(site['site_nice'].lower())
                update_datetime = site['last_update']
                update_datetime = datetime.datetime.fromtimestamp(update_datetime)
                rows['update_datetime'].append(update_datetime)
                rows['home_team_odds'].append(site['odds'][self.mkt][0])
                rows['away_team_odds'].append(site['odds'][self.mkt][1])
                rows['draw_odds'].append(site['odds'][self.mkt][2])
                rows['home_team'].append(home_team)
                rows['away_team'].append(away_team)

        df = pd.DataFrame(rows)
        return df



bookie_odds_collector = BookieOddsCollector(api_key=API_KEY,
                                            sport=SPORT,
                                            region=REGION,
                                            mkt=MARKET)

bookie_odds_collector.get_results()

# odds_response = requests.get('https://api.the-odds-api.com/v3/odds', params={
#     'api_key': API_KEY,
#     'sport': SPORT,
#     'region': REGION,
#     'mkt': MARKET,
# })
