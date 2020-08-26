import os
import bz2
import json
import datetime

import numpy as np
import pandas as pd


path = r'C:\Users\rober\sport_data\BASIC\correct_score\2017\Jan\1\28057867'
filename = "1.128873815.bz2"
file = os.path.join(path, filename)

# path = r'C:\Users\rober\sport_data\BASIC\event\2020\Jan\4\29609499'
# filename = '29609499.bz2'
# file = os.path.join(path, filename)


class ExchangeOddsExtractor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.definition_changes = None
        self.price_changes = None

    def _categorise_market_changes(self):
        with bz2.open(self.file_path, 'rt') as fp:
            definition_changes = []
            price_changes = []
            for line in fp:
                data = json.loads(line)
                published_time = datetime.datetime.fromtimestamp(data['pt'] / 1000)
                published_time = published_time.replace(second=0, microsecond=0)
                data = json.loads(line)
                market_changes = data['mc']
                for market_change in market_changes:
                    if 'marketDefinition' in market_change:
                        definition_changes.append((published_time, market_change))
                    elif 'rc' in market_change:
                        price_changes.append((published_time, market_change))
                    else:
                        raise ValueError("Unknown market change type")

            self.definition_changes = definition_changes
            self.price_changes = price_changes

    def extract_data(self):
        self._categorise_market_changes()
        event_data, market_data = self._get_event_and_market_data()

        # Get odds changes for all runners
        odds_df = pd.DataFrame(np.nan, index=pd.DatetimeIndex([x[0] for x in self.price_changes]),
                               columns=list(market_data['runner_ids'].values()))
        for published_datetime, price_change in self.price_changes:
            for rc in price_change['rc']:
                if rc['ltp'] != 1000:
                    odds_df.at[published_datetime, market_data['runner_ids'][rc['id']]] = rc['ltp']

        return event_data, market_data, odds_df

    def _get_event_and_market_data(self):
        event_names = []
        event_ids = []
        market_ids = []
        country_codes = []
        market_types = []
        in_play_start_datetime = None
        runners = None
        for published_time, market_definition_change in self.definition_changes:
            if runners is None:
                runners = market_definition_change['marketDefinition']['runners']
            market_ids.append(market_definition_change['id'])
            event_names.append(market_definition_change['marketDefinition']['eventName'])
            event_ids.append(market_definition_change['marketDefinition']['eventId'])
            country_codes.append(market_definition_change['marketDefinition']['countryCode'])
            market_types.append(market_definition_change['marketDefinition']['marketType'])
            if market_definition_change['marketDefinition']['inPlay'] is True and in_play_start_datetime is None:
                in_play_start_datetime = published_time

        if in_play_start_datetime is None:
            raise ValueError("No in play information found")

        runner_ids = {}
        for runner in runners:
            runner_ids[runner['id']] = runner['name']

        for data_item in [event_ids, event_names, country_codes, market_ids, market_types]:
            self._check_consistent_meta(data_item)

        team_a, team_b = event_names[0].lower().split(' v ')

        event_data = {'event_name': event_names[0],
                      'event_id': event_ids[0],
                      'country_code': country_codes[0],
                      'in_play_start_datetime': in_play_start_datetime,
                      'team_a': team_a,
                      'team_b': team_b
                      }

        market_data = {'market_id': market_ids[0],
                       'market_type': market_types[0],
                       'runner_ids': runner_ids}

        return event_data, market_data

    @staticmethod
    def _check_consistent_meta(data):
        if len(set(data)) != 1:
            raise ValueError("More than one eventname/id in market definition changes for single"
                             " data set")


extractor = ExchangeOddsExtractor(os.path.join(file))
event, market, odds = extractor.extract_data()

print(event['event_id'])
print(event['team_a'])
print(event['team_b'])
print(event['country_code'])
print(market['market_id'])
print(market['market_type'])
print(odds)

from orm.orm import Team, Country, Event, MarketType
from utils.database_manager import dbm

with dbm.get_managed_session() as session:
    country = Country.get_by_code(session, country_code=event['country_code'])
    team_a, existed = Team.create_or_update(session, team_name=event['team_a'],
                                   country=country)
    team_b, existed = Team.create_or_update(session, team_name=event['team_b'],
                                   country=country)
    event = Event.create_or_update(session, event['event_id'], team_a=team_a,
                                   team_b=team_b, in_play_start=event['in_play_start_datetime'])
