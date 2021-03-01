import json
import requests
import datetime
import pandas as pd
import os
import time
from tabulate import tabulate

from crusher.division import DivisionCodeEnum as DCEnum


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

    def log_odds_to_csv(self, csv_path, refresh_rate_seconds=60):
        starttime = time.time()
        while True:
            df = self.get_results()
            if not os.path.exists(csv_path):
                df.to_csv(csv_path, header=True, index=False)
            else:
                df.to_csv(csv_path, header=False, mode='a', index=False)
            print("Logged odds")
            time.sleep(refresh_rate_seconds - ((time.time() - starttime) % refresh_rate_seconds))

    def _parse_data_to_df(self, odds_data):
        rows = {'site': [],
                'home_team': [],
                'away_team': [],
                'match_date': [],
                'home_team_odds': [],
                'away_team_odds': [],
                'draw_odds': [],
                'update_datetime': []}
        for event in odds_data:
            teams = [team.lower() for team in event['teams']]
            home_team = event['home_team'].lower()
            away_team = [team for team in teams if team != home_team][0]
            home_team_ix = teams.index(home_team)
            away_team_ix = teams.index(away_team)
            match_date = event['commence_time']
            match_date = datetime.datetime.fromtimestamp(match_date)
            match_date = match_date.date()
            for site in event['sites']:
                rows['site'].append(site['site_nice'].lower())
                update_datetime = site['last_update']
                update_datetime = datetime.datetime.fromtimestamp(update_datetime)
                rows['update_datetime'].append(update_datetime)
                rows['home_team_odds'].append(site['odds'][self.mkt][home_team_ix])
                rows['away_team_odds'].append(site['odds'][self.mkt][away_team_ix])
                rows['draw_odds'].append(site['odds'][self.mkt][2])
                rows['home_team'].append(home_team)
                rows['away_team'].append(away_team)
                rows['match_date'].append(match_date)

        df = pd.DataFrame(rows)
        df = df.loc[df['match_date'] == datetime.date.today()]
        return df

    def stream_live_odds(self, refresh_rate_seconds=60, log_odds=False, save_path=None,
                         drop_betfair_from_live_stream=True, home_team_stream_filter=None,
                         away_team_stream_filter=None, match_date_stream_filter=None):
        starttime = time.time()
        while True:
            df = self.get_results()
            if log_odds:
                if not save_path:
                    raise ValueError("Need path to save odds")
                if not os.path.exists(save_path):
                    df.to_csv(save_path, header=True, index=False)
                else:
                    df.to_csv(save_path, header=False, mode='a', index=False)

            if drop_betfair_from_live_stream:
                df = df[df['site'] != 'betfair']
            info_df_grouped = df.groupby(['home_team', 'away_team', 'match_date'])
            info_df = info_df_grouped.agg(func=['mean', 'std'])
            info_df.columns = list(map('_'.join, info_df.columns.values))
            info_df = info_df.reset_index()
            info_df = info_df[['home_team', 'away_team', 'match_date',
                               'home_team_odds_mean', 'home_team_odds_std',
                               'away_team_odds_mean', 'away_team_odds_std',
                               'draw_odds_mean', 'draw_odds_std']]
            odds_columns = ['home_team_odds_mean', 'home_team_odds_std',
                            'away_team_odds_mean', 'away_team_odds_std',
                            'draw_odds_mean', 'draw_odds_std']
            info_df[odds_columns] = info_df[odds_columns].round(2)
            if home_team_stream_filter:
                info_df = info_df[info_df['home_team'] == home_team_stream_filter]
            if away_team_stream_filter:
                info_df = info_df[info_df['away_team'] == away_team_stream_filter]
            if match_date_stream_filter:
                info_df = info_df[info_df['match_date'] == match_date_stream_filter]
            print(tabulate(info_df, headers='keys', tablefmt='psql'))
            time.sleep(refresh_rate_seconds - ((time.time() - starttime) % refresh_rate_seconds))


the_odds_api_league_code = {DCEnum.PREMIER_LEAGUE: 'soccer_epl',
                            DCEnum.CHAMPIONSHIP: 'soccer_efl_champ',
                            DCEnum.LEAGUE_1: 'soccer_england_league1',
                            DCEnum.LEAGUE_2: 'soccer_england_league2',
                            DCEnum.FRANCE_LEAGUE_1: 'soccer_france_ligue_one',
                            DCEnum.GERMANY_BUNDESLIGA: 'soccer_germany_bundesliga',
                            DCEnum.GERMANY_BUNDESLIGA_2: 'soccer_germany_bundesliga2',
                            DCEnum.ITALY_SERIE_A: 'soccer_italy_serie_a',
                            DCEnum.ITALY_SERIE_B: 'soccer_italy_serie_b',
                            DCEnum.SPAIN_LA_LIGA: 'soccer_spain_la_liga',
                            DCEnum.SPAIN_SEGUNDA: 'soccer_spain_segunda_division'}


def convert_division_code_to_api(division_codes):
    api_codes = []
    for code in division_codes:
        api_codes.append(the_odds_api_league_code[code])
    return api_codes
