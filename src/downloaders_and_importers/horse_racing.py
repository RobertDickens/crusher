import os
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

from utils.db.database_manager import dbm
from utils.parsers.horse_racing_odds_parser import HorseRacingExchangeOddsExtractor
from orm.orm import Team, Country, Event, Market, ExchangeOddsSeries, Runner
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from crusher.sport import SportCodeEnum as SCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.info_source import InfoSourceEnum as ISEnum
from utils.db import db_table_names as tb

n_rows = 0
for month in ['Jan', 'Feb', 'Mar']:
    root_dir = "C:\\Users\\rober\\sport_data\\horse_racing\\ADVANCED\\2020\\"
    root_dir = os.path.join(root_dir, month)
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
                try:
                    with dbm.get_managed_session() as session:
                        runner_uid_from_code_map = {k: Runner.get_by_code(session, k).runner_uid for k in
                                                    RCEnum.to_dict().values()}
                        print(subdir)
                        extractor = HorseRacingExchangeOddsExtractor(os.path.join(subdir, file))
                        event_data, market_data, df, total_pre_off_volume, total_volume = extractor.extract_data()
                        event, _ = Event.create_or_update(session, event_data['event_id'], team_a=None, team_b=None,
                                                          sport_code=SCEnum.HORSE_RACING)
                        market, existed = Market.create_or_update(session, market_betfair_id=str(market_data['market_id']),
                                                                  market_type_code=MTCEnum.WIN, event=event,
                                                                  pre_off_volume=total_pre_off_volume, total_volume=total_volume,
                                                                  off_time=event_data['off_time'])
                        series, _ = ExchangeOddsSeries.create_or_update(session, event=event, market=market,
                                                                        item_freq_type_code=IFTCEnum.SECOND,
                                                                        info_source_code=ISEnum.EXCHANGE_HISTORICAL)
                        df['series_uid'] = series.series_uid
                        df.to_sql(name=tb.exchange_odds_series_item(), schema='public',
                                  con=session.connection(),
                                  if_exists='append', method='multi', index=False)
                        n_rows += len(df)
                        print(f"Added {len(df)} to db for total of {n_rows}")
                except Exception as exp:
                    print(exp)
