import bz2
import json
import datetime

import numpy as np
import pandas as pd
from crusher.runner import horse_racing_runner_map


class HorseRacingExchangeOddsExtractor:
    """For parsing betfair historical data into market, event and odds
    data."""
    def __init__(self, file_path, min_volume=None):
        self.file_path = file_path
        self.min_volume = min_volume
        self.definition_changes = None
        self.price_changes = None
        self.traded_volume = None

    def _categorise_market_changes(self):
        """Iterates through API messages and categorisings into market changes
        and price changes"""
        with bz2.open(self.file_path, 'rt') as fp:
            definition_changes = []
            price_changes = []
            traded_volume = []
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
                    elif 'tv' in market_change:
                        traded_volume.append((published_time, market_change))
                    else:
                        raise ValueError("Unknown market change type")

            self.definition_changes = definition_changes
            self.price_changes = price_changes
            self.traded_volume = traded_volume

    def extract_data(self):
        """Extract data from a bz2 file"""
        self._categorise_market_changes()
        event_data, market_data = self._get_event_and_market_data()

        # Get odds changes for all runners
        all_unique_datetimes = list(set([x[0] for x in self.price_changes]))
        odds_df = pd.DataFrame(np.nan, index=pd.DatetimeIndex(all_unique_datetimes),
                               columns=list(market_data['runner_ids'].values()))
        for published_datetime, price_change in self.price_changes:
            for rc in price_change['rc']:
                if rc['ltp'] < 800 and rc['ltp'] != 0:
                    odds_df.at[published_datetime, market_data['runner_ids'][rc['id']]] = rc['ltp']

        # Get traded volume for all runners
        volume_df = odds_df.copy(deep=True)
        for published_datetime, price_change in self.price_changes:
            for rc in price_change['rc']:
                volume_df.at[published_datetime, market_data['runner_ids'][rc['id']]] = rc['tv']

        return event_data, market_data, odds_df, volume_df

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
            raise ValueError(f"Ignoring match {event_names[0]} since no off_time information found")

        runner_ids = {}
        for runner in runners:
            runner_ids[runner['sortPriority']] = horse_racing_runner_map[runner['sortPriority']]

        # for data_item in [event_ids, event_names, country_codes, market_ids, market_types]:
        #     self._check_consistent_meta(data_item)

        event_data = {'event_name': event_names[0],
                      'event_id': event_ids[0],
                      'country_code': country_codes[0],
                      'off_time': in_play_start_datetime,
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
