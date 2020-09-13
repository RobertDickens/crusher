from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Runner, ExchangeOddsSeries
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.division import DivisionCodeEnum
from utils.helper_functions import preprocessing as pr
from utils.helper_functions import calculation as calc
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import pandas as pd


# Modelling P/L from under 2.5 goals
def get_data():
    with dbm.get_managed_session() as session:
        # Get data
        runner = Runner.get_by_code(session, RCEnum.UNDER_2_5_GOALS)
        df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                        division_code=[DivisionCodeEnum.PREMIER_LEAGUE,
                                                                       DivisionCodeEnum.CHAMPIONSHIP],
                                                        runner_uid=runner.runner_uid,
                                                        in_play=True,
                                                        item_freq_type_code=IFTCEnum.MINUTE
                                                        )
        return df

# Investiage the slope of the odds drop off
df = get_data()
df = df.sort_values(['series_uid', 'published_datetime'], ascending=True)
df = pr.convert_published_datetime_to_game_time(df)
df = df[df['game_time'] <= 30]
df = pr.drop_series_with_price_jumps(df, 0.5)
df = pr.drop_outlier_series_by_std(df, 4)
df = df[['series_uid', 'game_time', 'ltp']]
df = pr.normalise_in_play_ltp(df)

slope_df = pd.DataFrame(columns=['start_ltp', 'slope'])
for series_uid in df['series_uid'].unique():
    sub_df = df[df['series_uid'] == series_uid]
    lr = LinearRegression(normalize=False, fit_intercept=True)
    lr.fit(sub_df['game_time'].values.reshape(-1, 1),
           sub_df['normalised_ltp'].values.reshape(-1, 1))
    slope = lr.coef_[0][0]
    slope_df = slope_df.append({'start_ltp': sub_df['ltp'].max(),
                                'slope': slope}, ignore_index=True)

slope_df = slope_df.sort_values('start_ltp', ascending=True)
plt.scatter(slope_df['start_ltp'], slope_df['slope'])
plt.show()


def backtest_back_lay(back_stake, odds_df, lay_time, back_time, starting_bankroll):
    bankroll = starting_bankroll
    bankroll_record = []
    for series_uid in odds_df['series_uid'].unique():
        bankroll_record.append(bankroll)
        sub_df = odds_df[odds_df['series_uid'] == series_uid]
        back_odds = sub_df[sub_df['game_time'] == back_time]['ltp'].values[0]
        if back_odds < 1.8:
            try:
                lay_odds = sub_df[sub_df['game_time'] == lay_time]['ltp'].values[0]
                lay_stake = calc.equal_hedge(back_odds=back_odds, lay_odds=lay_odds,
                                             back_stake=back_stake)
                profit = calc.profit(back_odds=back_odds, lay_odds=lay_odds,
                                     back_stake=back_stake, lay_stake=lay_stake)[0]
            except:
                profit = -back_stake
            if profit > 0:
                profit = 0.95 * profit
            bankroll = bankroll + profit
            print(profit)
        else:
            continue

    return bankroll_record

df = get_data()
df = pr.convert_published_datetime_to_game_time(df)
bankroll_record_ = backtest_back_lay(100, odds_df=df, back_time=0, lay_time=20,
                                     starting_bankroll=1000)
plt.plot(bankroll_record_)
plt.show()
