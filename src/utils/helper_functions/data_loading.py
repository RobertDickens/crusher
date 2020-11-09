import pandas as pd
import numpy as np
from datetime import datetime
import json

from orm.orm import ExchangeOddsSeriesItem, Runner, MarketType
from utils.helper_functions import preprocessing as pr
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from utils.db.database_manager import dbm
from crusher.runner import horse_racing_runner_map


def get_processed_horse_racing_odds(runner_codes=None, **kwargs):
    """Return dataframe of horse racing odds in a user friendly format
    i.e. with columns as individuals horse odds or volume"""
    with dbm.get_managed_session() as session:
        inverse_horse_racing_map = {v: k for k, v in horse_racing_runner_map.items()}
        if runner_codes is None:
            runner_uids = horse_racing_runner_map.keys()
        else:
            runner_uids = [inverse_horse_racing_map[code] for code in runner_codes]

        df = ExchangeOddsSeriesItem.get_series_items_df(session, from_date=datetime(2020, 1, 1),
                                                        until_date=datetime(2020, 1, 2),
                                                        min_market_pre_off_volume=150000,
                                                        max_mins_from_off_time=180)
        df['update_dict'] = df['update_json'].apply(lambda j: json.loads(j))
        df = df.drop('update_json', axis=1)
        # Add ltp columns
        df[[runner_uid for runner_uid in runner_uids]] = np.nan
        for runner_uid in runner_uids:
            df[runner_uid] = df['update_dict'].apply(lambda d: d['ltp'].get(str(runner_uid)))
            df[runner_uid] = df[runner_uid].fillna(method='ffill')
            df[runner_uid] = df[runner_uid].fillna(method='bfill')
            df = df.rename(columns={runner_uid: (horse_racing_runner_map[int(runner_uid)] + '_ltp').lower()})





df_ = get_processed_horse_racing_odds()


def get_odds_data(session, in_play, item_freq_type_code,
                  runner=None, market_type_code=None, convert_to_game_time=False):
    if runner:
        runner = Runner.get_by_code(session, runner).runner_uid
    df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                    market_type_code=market_type_code,
                                                    in_play=in_play,
                                                    item_freq_type_code=item_freq_type_code)
    df = df.sort_values(['series_uid', 'published_datetime'], ascending=True).reset_index(drop=True)
    if convert_to_game_time:
        df = pr.convert_published_datetime_to_game_time(df)
        df = df[['series_uid', 'game_time', 'ltp']]
    df = df[['series_uid', 'published_datetime', 'ltp']]
    return df


def get_match_odds_data(session, in_play, item_freq_type_code,
                        convert_to_game_time=False):
    df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                    market_type_code=MTCEnum.MATCH_ODDS,
                                                    in_play=in_play,
                                                    item_freq_type_code=item_freq_type_code)
    df = df.sort_values(['series_uid', 'published_datetime'], ascending=True).reset_index(drop=True)

    runner_map = {k: Runner.get_by_uid(session, int(k)).runner_code.lower()
                  for k in df['runner_uid'].unique()}
    df['runner_code'] = df['runner_uid'].map(runner_map)
    df = df.drop('runner_uid', axis=1)
    df = df.pivot(index=['series_uid', 'published_datetime', 'ltp'], columns='runner_code', values='ltp')
    df = df.reset_index()
    df = df.rename_axis(None, axis=1)

    for series_uid in df['series_uid'].unique():
        sub_df = df[df['series_uid'] == series_uid]
        sub_df = sub_df.fillna(method='ffill')
        sub_df = sub_df.fillna(method='bfill')
        df.at[sub_df.index, ['team_a', 'team_b', 'the_draw']] = sub_df[['team_a', 'team_b', 'the_draw']]

    if convert_to_game_time:
        df = pr.convert_published_datetime_to_game_time(df)
        df = df[['series_uid', 'game_time', 'team_a', 'team_b', 'the_draw']]
    else:
        df = df[['series_uid', 'published_datetime', 'team_a', 'team_b', 'the_draw']]

    df = df.drop_duplicates(['published_datetime', 'series_uid'], keep='last')

    return df
