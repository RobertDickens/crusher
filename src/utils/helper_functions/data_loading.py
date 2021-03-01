import pandas as pd
import numpy as np
from datetime import datetime
import json
import matplotlib.pyplot as plt

from crusher.runner import RunnerCodeEnum as RCEnum
from orm.orm import ExchangeOddsSeriesItem, Runner, MarketType
from utils.helper_functions import preprocessing as pr
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from utils.db.database_manager import dbm
from crusher.runner import horse_racing_runner_map


def get_processed_horse_racing_odds(runner_codes=None, from_date=None, until_date=None,
                                    market_type_code=None, division_codes=None,
                                    item_freq_type_code=None, min_market_total_volume=None,
                                    min_market_pre_off_volume=None, max_mins_from_off_time=60,
                                    market_location_code=None):
    """Return dataframe of horse racing odds in a user friendly format
    i.e. with columns as individuals horse odds or volume"""
    with dbm.get_managed_session() as session:
        inverse_horse_racing_map = {v: k for k, v in horse_racing_runner_map.items()}
        if runner_codes is None:
            runner_uids = horse_racing_runner_map.keys()
        else:
            runner_uids = [inverse_horse_racing_map[code] for code in runner_codes]

        df = ExchangeOddsSeriesItem.get_series_items_df(session, from_date=from_date, until_date=until_date,
                                                        market_type_code=market_type_code,
                                                        division_codes=division_codes,
                                                        item_freq_type_code=item_freq_type_code,
                                                        min_market_total_volume=min_market_total_volume,
                                                        min_market_pre_off_volume=min_market_pre_off_volume,
                                                        max_mins_from_off_time=max_mins_from_off_time,
                                                        market_location_code=market_location_code)
        df['update_dict'] = df['update_json'].apply(lambda j: json.loads(j))
        df = df.drop('update_json', axis=1)
        df = df.sort_values('published_datetime', ascending=True)

        # Add ltp and tv columns
        df[[runner_uid for runner_uid in runner_uids]] = np.nan
        for runner_uid in runner_uids:
            ltp_col_name = (horse_racing_runner_map[int(runner_uid)] + '_ltp').lower()
            tv_col_name = (horse_racing_runner_map[int(runner_uid)] + '_tv').lower()
            df[ltp_col_name] = df['update_dict'].apply(lambda d: d['ltp'].get(str(runner_uid)))
            df[tv_col_name] = df['update_dict'].apply(lambda d: d['tv'].get(str(runner_uid)))

            # fill nans
            for series_uid in df['series_uid'].unique():
                series_ix = df['series_uid'] == series_uid
                df.loc[series_ix, ltp_col_name] = df.loc[series_ix, ltp_col_name].fillna(method='ffill')
                df.loc[series_ix, ltp_col_name] = df.loc[series_ix, ltp_col_name].fillna(method='bfill')

                df.loc[series_ix, tv_col_name] = df.loc[series_ix, tv_col_name].fillna(method='ffill')
                df.loc[series_ix, tv_col_name] = df.loc[series_ix, tv_col_name].fillna(method='bfill')

            # drop if column is all nan
                if df[ltp_col_name].isna().all():
                     df = df.drop([ltp_col_name, tv_col_name], axis=1)
                else:
                # convert to float
                    df[ltp_col_name] = df[ltp_col_name].apply(lambda x: float(x) if x else None)
                    df[tv_col_name] = df[tv_col_name].apply(lambda x: float(x) if x else None)

        # Drop unnecessary columns
        df = df.drop('update_dict', axis=1)
        df = df.drop([runner_uid for runner_uid in runner_uids], axis=1)
        df = df.dropna(axis=1, how='all')
        df = df.drop_duplicates(['series_uid', 'published_datetime'])

        return df
