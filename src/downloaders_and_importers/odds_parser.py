import os
import bz2
import json
import datetime
from matplotlib import pyplot as plt

import numpy as np
import pandas as pd


path = r'C:\Users\rober\sport_data\BASIC\correct_score\2017\Jan\1\28057867'
filename = "1.128873815.bz2"
file = os.path.join(path, filename)


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
        # Get metadata
        market_definition = self.definition_changes[0][1]['marketDefinition']
        event_name = market_definition['eventName']
        market_name = market_definition['name']
        country_code = market_definition['countryCode']
        metadata = {'event_name': event_name,
                    'market_name': market_name,
                    'country_code': country_code}

        runner_ids = {}
        for runner in market_definition['runners']:
            runner_ids[runner['id']] = runner['name']

        # Get odds changes for all runners
        odds_df = pd.DataFrame(np.nan, index=pd.DatetimeIndex([x[0] for x in self.price_changes]),
                               columns=list(runner_ids.values()))
        for published_datetime, price_change in self.price_changes:
            for rc in price_change['rc']:
                if rc['ltp'] != 1000:
                    odds_df.at[published_datetime, runner_ids[rc['id']]] = rc['ltp']

        return metadata, odds_df


extractor = ExchangeOddsExtractor(os.path.join(file))
metadata, df = extractor.extract_data()
print(df)
