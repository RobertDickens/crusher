from orm.orm import ExchangeOddsSeriesItem, Runner, MarketType
from utils.helper_functions import preprocessing as pr
from crusher.market_type import MarketTypeCodeEnum as MTCEnum


def get_odds_data(session, divisions, in_play, item_freq_type_code,
                  runner=None, market_type_code=None, convert_to_game_time=False):
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


def get_match_odds_data(session, divisions, in_play, item_freq_type_code,
                        convert_to_game_time=False):
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
        df = df[['series_uid', 'game_time', 'team_a', 'team_b', 'the_draw']]
    else:
        df = df[['series_uid', 'published_datetime', 'team_a', 'team_b', 'the_draw']]

    df = df.drop_duplicates(['published_datetime', 'series_uid'], keep='last')

    return df


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
import numpy as np


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

# get data
with dbm.get_managed_session() as session:
    df = get_joined_match_score_odds_df(session)
# iterate through events
bankroll = 10000
for event_uid in df['event_uid'].unique():
    # iterate through the items looking for match odds diff changes
    sub_df = df[df['event_uid'] == event_uid]
    last_datetime = max(sub_df.index)
    start_datetime = last_datetime - relativedelta(days=2)
    sub_df = sub_df.sort_index(ascending=True)
    sub_df = sub_df[sub_df.index > start_datetime]
    sub_df = sub_df.resample('min').asfreq()
    sub_df = sub_df.fillna(method='ffill')
    sub_df['match_odds_diff_1_mins'] = sub_df['match_odds_diff'] - sub_df['match_odds_diff'].shift(1)
    sub_df['score_odds_diff_1_mins'] = sub_df['score_odds'] - sub_df['score_odds'].shift(1)
    sub_df = sub_df.dropna()
    back_ixs = []
    for ix, row in sub_df.iterrows():
        if row['match_odds_diff_1_mins'] < -0.6 and row['score_odds_diff_1_mins'] == 0:
            back_ixs.append(ix)
        else:
            continue

    if back_ixs:
        back_odds = sub_df.loc[back_ixs[0]]['score_odds']
        df_after_ix = sub_df[sub_df.index > back_ixs[0]]
        values = df_after_ix['score_odds'].values
        try:
            lay_odds = next(v for v in values if v < back_odds)
        except:
            lay_odds = None
        if lay_odds is not None:
            lay_stake = calc.equal_hedge(back_odds, lay_odds, 100)
            profit = (calc.profit(back_stake=100,
                                  lay_stake=lay_stake,
                                  back_odds=back_odds,
                                  lay_odds=lay_odds)[0])
        else:
            lay_odds = df_after_ix.iloc[-1]['score_odds']
            lay_stake = calc.equal_hedge(back_odds, lay_odds, 100)
            profit = (calc.profit(back_stake=100,
                                  lay_stake=lay_stake,
                                  back_odds=back_odds,
                                  lay_odds=lay_odds)[0])

        bankroll += profit
        print(bankroll)
