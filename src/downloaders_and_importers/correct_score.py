import os
from datetime import datetime

import pandas as pd

from orm.orm import Team, Country, Event, Market, ExchangeOddsSeries, ExchangeOddsSeriesItem, Runner
from utils.db.database_manager import dbm
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.info_source import InfoSourceEnum as ISEnum
from crusher.runner import runner_betfair_map
from utils.parsers.odds_parser import ExchangeOddsExtractor
from utils.db import db_table_names as tb


root_dir = "C:\\Users\\rober\\sport_data\\BASIC\\correct_score\\BASIC\\2018\\Feb\\"
# extractor = ExchangeOddsExtractor(os.path.join(dirpath))
# event_data, market_data, odds = extractor.extract_data()

with dbm.get_managed_session() as session:
    runner_uid_from_code_map = {k: Runner.get_by_code(session, k).runner_uid for k in RCEnum.to_dict().values()}

# # for month in ['Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']:
for subdir, dirs, files in os.walk(root_dir):
    for file in files:
        with dbm.get_managed_session() as session:
            try:
                extractor = ExchangeOddsExtractor(os.path.join(subdir, file), min_ltp_data_points=5)
                event_data, market_data, odds = extractor.extract_data()
                country = Country.get_by_code(session, country_code=event_data['country_code'])
                team_a, _ = Team.create_or_update(session, team_name=event_data['team_a'],
                                                  country=country)
                team_b, _ = Team.create_or_update(session, team_name=event_data['team_b'],
                                                  country=country)
                event, _ = Event.create_or_update(session, event_data['event_id'], team_a=team_a,
                                                  team_b=team_b, in_play_start=event_data['in_play_start_datetime'])
                market, _ = Market.create_or_update(session, market_betfair_id=str(market_data['market_id']),
                                                    market_type_code=MTCEnum.CORRECT_SCORE, event=event)
                series, _ = ExchangeOddsSeries.create_or_update(session, event=event, market=market,
                                                                item_freq_type_code=IFTCEnum.MINUTE,
                                                                info_source_code=ISEnum.EXCHANGE_HISTORICAL)
                session.commit()
                items_df = odds.melt(ignore_index=False).reset_index().dropna()
                items_df = items_df.rename(columns={'index': 'published_datetime',
                                                    'value': 'ltp',
                                                    'variable': 'runner_code'})
                items_df['runner_code'] = items_df['runner_code'].apply(lambda c: runner_betfair_map[c])
                items_df['runner_uid'] = items_df['runner_code'].apply(lambda c: runner_uid_from_code_map[c])
                items_df = items_df.drop('runner_uid', axis=1)
                items_df['series_uid'] = series.series_uid
                items_df['in_play'] = items_df['published_datetime'] >= event_data['in_play_start_datetime']
                items_df['update_datetime'] = datetime.utcnow()
                items_df['creation_datetime'] = datetime.utcnow()

                items_df.to_sql(name=tb.exchange_odds_series_item(), schema='public',
                                con=session.connection(),
                                if_exists='append', method='multi', index=False)
                print(f"Successfully imported CORRECT_SCORE historical data for {event_data['event_name']}")
            except Exception as exc:
                print(exc)
                continue
