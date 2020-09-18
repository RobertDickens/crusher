from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Runner, MarketType
from utils.helper_functions import preprocessing as pr
from crusher.market_type import MarketTypeCodeEnum as MTCEnum


def get_odds_data(divisions, in_play, item_freq_type_code,
                  runner=None, market_type_code=None, convert_to_game_time=False):
    with dbm.get_managed_session() as session:
        if runner:
            runner = Runner.get_by_code(session, runner).runner_uid
        df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                        division_code=divisions,
                                                        runner_uid=runner,
                                                        market_type_code=market_type_code,
                                                        in_play=in_play,
                                                        item_freq_type_code=item_freq_type_code)
        df = df.sort_values(['series_uid', 'published_datetime'], ascending=True).reset_index(drop=True)
        if convert_to_game_time:
            df = pr.convert_published_datetime_to_game_time(df)
            df = df[['series_uid', 'game_time', 'ltp']]
        df = df[['series_uid', 'published_datetime', 'ltp']]
        return df


def get_match_odds_data(divisions, in_play, item_freq_type_code,
                        convert_to_game_time=False):
    with dbm.get_managed_session() as session:
        df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                        division_code=divisions,
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
            df = df[['series_uid', 'game_time', 'ltp', 'team_a', 'team_b', 'the_draw']]
        else:
            df = df[['series_uid', 'published_datetime', 'ltp', 'team_a', 'team_b', 'the_draw']]

        return df

from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from crusher.division import DivisionCodeEnum as DCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from utils.helper_functions import calculation as calc
from matplotlib import pyplot as plt
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.db.database_manager import dbm
from utils.helper_functions import preprocessing as pr
from orm.orm import ExchangeOddsSeries

df_match_odds = get_match_odds_data(divisions=[DCEnum.PREMIER_LEAGUE],
                                    item_freq_type_code=IFTCEnum.MINUTE,
                                    in_play=False)
df_match_odds = pr.drop_series_by_n_data_points(df_match_odds, 40)
df_match_odds['match_odds_diff'] = abs(df_match_odds['team_a'] - df_match_odds['team_b'])
print(df_match_odds)
for series_uid in df_match_odds['series_uid'].unique():
    sub_df = df_match_odds[df_match_odds['series_uid'] == series_uid]
    sub_df['match_odds_diff'].plot()
    plt.show()
