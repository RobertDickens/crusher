import json
import requests
import datetime
import pandas as pd
import os
import time


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

    def log_odds_to_csv(self, csv_path, refresh_rate_minutes=60):
        starttime = time.time()
        while True:
            df = self.get_results()
            if not os.path.exists(csv_path):
                df.to_csv(csv_path, header=True, index=False)
            else:
                df.to_csv(csv_path, header=False, mode='a', index=False)
            print("Logged odds")
            time.sleep(refresh_rate_minutes - ((time.time() - starttime) % refresh_rate_minutes))

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

