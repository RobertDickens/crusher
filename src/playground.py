from utils.helper_functions.data_loading import get_odds_data, get_match_odds_data
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.market_type import MarketTypeCodeEnum as MTCEnum
from crusher.division import DivisionCodeEnum as DCEnum
from utils.db.database_manager import dbm
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from utils.helper_functions import calculation as calc
from matplotlib import pyplot as plt
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.db.database_manager import dbm
from utils.helper_functions import preprocessing as pr
from orm.orm import ExchangeOddsSeries
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
from utils.helper_functions import calculation as calc
import numpy as np
from utils.helper_functions.backtest import BacktestStrategy
from models.correct_score_0_0.simple_strategies import naive_high_odds_grabber_by_ticks, \
    naive_high_odds_grabber_by_percentage


def get_nil_nil_odds_df(session):
    df = get_odds_data(session=session,
                       runner=RCEnum.SCORE_0_0,
                       divisions=[DCEnum.PREMIER_LEAGUE, DCEnum.CHAMPIONSHIP],
                       item_freq_type_code=IFTCEnum.MINUTE,
                       in_play=False)
    df = pr.add_event_uid_to_df(df)
    df = pr.drop_outlier_series_by_std(df)
    df = pr.drop_series_by_n_data_points(df, 40)
    return df


def get_match_odds_df(session):
    df_match_odds = get_match_odds_data(session=session,
                                        divisions=[DCEnum.PREMIER_LEAGUE],
                                        item_freq_type_code=IFTCEnum.MINUTE,
                                        in_play=False)
    df_match_odds = pr.drop_series_by_n_data_points(df_match_odds, 40)
    df_match_odds = pr.drop_outlier_series_by_std(df_match_odds, 3, value_col='team_a')
    df_match_odds = pr.add_event_uid_to_df(df_match_odds)
    df_match_odds['match_odds_diff'] = abs(df_match_odds['team_a'] - df_match_odds['team_b'])

    return df_match_odds


def get_joined_match_score_odds_df(session):
    df_score = get_nil_nil_odds_df(session)
    df_match = get_match_odds_df(session)
    df_score = df_score.drop('series_uid', axis=1)
    df_match = df_match.drop('series_uid', axis=1)

    df = pd.DataFrame(columns=['score_odds', 'match_odds_diff', 'event_uid'])
    for event_uid in df_match['event_uid'].unique():
        if event_uid not in df_score['event_uid'].unique():
            continue
        sub_match_df = df_match[df_match['event_uid'] == event_uid]
        sub_match_df = sub_match_df[['event_uid', 'match_odds_diff', 'published_datetime']]
        sub_match_df = sub_match_df.set_index('published_datetime')

        sub_score_df = df_score[df_score['event_uid'] == event_uid]
        sub_score_df = sub_score_df[['ltp', 'published_datetime']]
        sub_score_df = sub_score_df.set_index('published_datetime')

        joined_df = sub_score_df.join(sub_match_df, how='outer')
        joined_df[['ltp', 'match_odds_diff']] = joined_df[['ltp', 'match_odds_diff']].fillna(method='ffill')
        joined_df[['ltp', 'match_odds_diff']] = joined_df[['ltp', 'match_odds_diff']].fillna(method='bfill')
        joined_df['event_uid'] = event_uid
        joined_df = joined_df.rename(columns={'ltp': 'score_odds'})
        df = pd.concat([df, joined_df])

    return df


with dbm.get_managed_session() as session:
    df = get_joined_match_score_odds_df(session)

# Grid search high odds grabbing
profit = []
backtest = BacktestStrategy(odds_df=df, initial_bankroll=1000)
backtest.run_strategy(naive_high_odds_grabber_by_percentage, start_strategy_offset={'days': 1},
                      odds_percentage=110, back_stake=100,
                      odds_column_name='score_odds')
profit.append(backtest.results['profit'])

