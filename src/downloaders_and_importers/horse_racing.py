import os
import pandas as pd
from dateutil.relativedelta import relativedelta

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
for month in ['Jan']:
    root_dir = "C:\\Users\\rober\\sport_data\\horse_racing\\ADVANCED\\2020\\"
    root_dir = os.path.join(root_dir, month)
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
                try:
                    with dbm.get_managed_session() as session:
                        runner_uid_from_code_map = {k: Runner.get_by_code(session, k).runner_uid for k in
                                                    RCEnum.to_dict().values()}
                        extractor = HorseRacingExchangeOddsExtractor(os.path.join(subdir, file))
                        event_data, market_data, odds, volume = extractor.extract_data()
                        pre_off_volume = volume.loc[volume.index < event_data['off_time']]
                        total_pre_off_volume = pre_off_volume.max(axis=0).sum()
                        total_volume = volume.max(axis=0).sum()
                        event, _ = Event.create_or_update(session, event_data['event_id'], team_a=None, team_b=None,
                                                          sport_code=SCEnum.HORSE_RACING)
                        market, existed = Market.create_or_update(session, market_betfair_id=str(market_data['market_id']),
                                                            market_type_code=MTCEnum.WIN, event=event,
                                                            pre_off_volume=total_pre_off_volume, total_volume=total_volume,
                                                            off_time=event_data['off_time'])

                        series, _ = ExchangeOddsSeries.create_or_update(session, event=event, market=market,
                                                                        item_freq_type_code=IFTCEnum.SECOND,
                                                                        info_source_code=ISEnum.EXCHANGE_HISTORICAL)
                        odds_df = odds.melt(ignore_index=False).reset_index().dropna()
                        odds_df = odds_df.rename(columns={'index': 'published_datetime',
                                                          'value': 'ltp',
                                                          'variable': 'runner_code'})
                        volume_df = volume.melt(ignore_index=False).reset_index().dropna()
                        volume_df = volume_df.rename(columns={'index': 'published_datetime',
                                                              'value': 'traded_volume',
                                                              'variable': 'runner_code'})
                        items_df = pd.merge(odds_df,
                                            volume_df,
                                            how='left',
                                            left_on=['published_datetime', 'runner_code'],
                                            right_on=['published_datetime', 'runner_code'])
                        items_df['ltp'] = items_df['ltp'].apply(lambda x: round(x, 2))
                        items_df['traded_volume'] = items_df['traded_volume'].apply(lambda x: round(x, 2))
                        items_df['runner_uid'] = items_df['runner_code'].apply(lambda c: runner_uid_from_code_map[c])
                        items_df = items_df.drop('runner_code', axis=1)
                        items_df['series_uid'] = series.series_uid
                        items_df['in_play'] = items_df['published_datetime'] >= event_data['off_time']
                        items_df.to_sql(name=tb.exchange_odds_series_item(), schema='public',
                                        con=session.connection(),
                                        if_exists='append', method='multi', index=False)
                        n_rows += len(items_df)
                        print(f"Added {len(items_df)} to db for total of {n_rows}")
                except Exception as exp:
                    print(exp)
