import bz2
import json
import datetime

import numpy as np
import pandas as pd


class ResultsParser:
    """For parsing betfair historical data into market, event and odds
    data."""
    def __init__(self, file_path, ignore_youth_leages=True, ignore_reserves=True,
                 ignore_w=True, min_ltp_data_points=None):
        self.file_path = file_path
        self.ignore_youth_leages = ignore_youth_leages
        self.ignore_reserves = ignore_reserves
        self.ignore_w = ignore_w
        self.definition_changes = None
        self.min_ltp_data_points = min_ltp_data_points

    def _categorise_market_changes(self):
        """Iterates through API messages and categorisings into market changes
        and price changes"""
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

            self.definition_changes = definition_changes

    def extract_data(self):
        """Extract data from a bz2 file"""
        self._categorise_market_changes()
        return self._get_event_data()

    def _get_event_data(self):
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

        if self.ignore_youth_leages:
            if any(string in event_names[0] for string in ['U23', 'U21', 'U20', 'U19']):
                raise ValueError(f"Ignoring match: {event_names[0]}")
        if self.ignore_reserves:
            if '(res)' in event_names[0]:
                raise ValueError(f"Ignoring match: {event_names[0]}")
        if self.ignore_w:
            if '(W)' in event_names[0]:
                raise ValueError(f"Ignoring match {event_names[0]}")

        if in_play_start_datetime is None:
            raise ValueError(f"Ignoring match {event_names[0]} since no in play information found")

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

        return event_data

    @staticmethod
    def _check_consistent_meta(data):
        if len(set(data)) != 1:
            raise ValueError("More than one eventname/id in market definition changes for single"
                             " data set")
