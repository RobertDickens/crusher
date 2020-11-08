import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from dateutil.relativedelta import relativedelta
from orm.orm import ExchangeOddsSeriesItem, Market, Runner, Event, ExchangeOddsSeries
from utils.db.database_manager import dbm
from crusher.sport import SportCodeEnum as SCEnum
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.market_type import MarketTypeCodeEnum as MTCEnum

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import GroupKFold
from sklearn.metrics import cohen_kappa_score, accuracy_score, balanced_accuracy_score


# Get win markets where preoff volume > 100k
def classify_odds_movements(odds_pct_change):
    if odds_pct_change < -0.05:
        return 'short'
    if odds_pct_change > 0.05:
        return 'drift'
    else:
        return 'oscillate'


def refactor_data(df, off_time, resample_freq=None, n_lags=5):
    df = df.loc[df['published_datetime'] > off_time - relativedelta(minutes=180)]
    if df.empty:
        return None
    df = df.drop(['series_uid', 'series_item_uid', 'runner_uid', 'in_play'], axis=1)
    df = df.sort_values('published_datetime', ascending=True)
    df = df.set_index('published_datetime')
    df['min_to_off'] = df.index.map(lambda x: abs((x - off_time).total_seconds() / 60.0))
    if resample_freq:
        df = df.resample(resample_freq).mean()
    df['volume_change'] = df['traded_volume'].diff()
    df['odds_pct_change'] = df['ltp'].pct_change()
    df['odds_change'] = df['ltp'].diff()
    df['volume_pct_change'] = df['traded_volume'].pct_change()
    df['rolling_max'] = df['ltp'].expanding().max()
    df['rolling_min'] = df['ltp'].expanding().min()
    df['rolling_previous_max'] = df['rolling_max'].shift()
    df['rolling_previous_min'] = df['rolling_min'].shift()
    df['previous_ltp'] = df['ltp'].shift()
    df['odds_movement'] = df['odds_pct_change'].apply(lambda x: classify_odds_movements(x))
    df['series_uid'] = series_uid

    for n in range(1, n_lags):
        df[f'lag_ltp_{n}'] = df['ltp'].shift(n)
        df[f'volume_change_lag_{n}'] = df['volume_change'].shift(n)
        df[f'odds_change_pct_lag_{n}'] = df['odds_pct_change'].shift(n)
        df[f'odds_change_lag_{n}'] = df['odds_change'].shift(n)
        df[f'volume_pct_change_lagged_{n}'] = df['volume_pct_change'].shift(n)
    df = df.dropna()

    # fig, ax = plt.subplots()
    # colors = {'drift': 'green', 'short': 'blue', 'oscillate': 'grey'}
    # ax.scatter(df.index, df['ltp'], c=df['odds_movement'].apply(lambda x: colors[x]))
    # plt.show()

    return df


with dbm.get_managed_session() as session:
    raw_df = ExchangeOddsSeriesItem.get_series_items_df(session, market_type_code=MTCEnum.WIN,
                                                        in_play=False, min_market_pre_off_volume=100000,
                                                        runner_codes=[RCEnum.FAVOURITE])
    # Create features
    df = pd.DataFrame()
    for series_uid in raw_df['series_uid'].unique():
        sub_df = raw_df[raw_df['series_uid'] == series_uid]
        off_time = ExchangeOddsSeries.get_by_uid(session, int(series_uid)).market.off_time
        sub_df = refactor_data(sub_df, off_time, '10s', n_lags=10)
        if sub_df is not None:
            df = df.append(sub_df)
    del raw_df

    df = df.drop(['ltp', 'traded_volume', 'odds_pct_change', 'volume_change', 'rolling_max', 'rolling_min',
                  'volume_pct_change', 'odds_change'], axis=1)
    df = df[df['min_to_off'] < 5]
    groups = df['series_uid'].values
    X = df.drop(['odds_movement'], axis=1)
    #X = X[['previous_ltp', 'rolling_previous_max', 'odds_change_lag_1', 'volume_pct_change_lagged_1']]
    y = df[['odds_movement']]
    kf = GroupKFold(n_splits=3)

    for train_index, test_index in kf.split(X, y, groups=groups):
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        for series_uid in X_train['series_uid'].unique():
            if series_uid in X_test['series_uid'].unique():
                raise ValueError("That's a bad a split!")
        X_train = X_train.drop('series_uid', axis=1)
        X_test = X_test.drop('series_uid', axis=1)
        dt = GradientBoostingClassifier(n_estimators=100)
        dt.fit(X_train, y_train)
        predictions = dt.predict(X_test)
        feature_importances = dict(zip(X_train.columns, dt.feature_importances_))
        kappa_score = cohen_kappa_score(predictions, y_test)
        accuracy_score_ = accuracy_score(predictions, y_test)
        balanced_accuracy_score_ = balanced_accuracy_score(predictions, y_test)
        print(kappa_score)
        print(accuracy_score_)
        print(balanced_accuracy_score_)
