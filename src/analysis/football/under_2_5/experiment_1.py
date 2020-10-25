from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Runner, ExchangeOddsSeries
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.division import DivisionCodeEnum
from utils.helper_functions import preprocessing as pr
from utils.helper_functions import calculation as calc
from utils.helper_functions.data_loading import get_odds_data
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import pandas as pd


def get_minute_of_price_jumps(df, threshhold):
    jump_times = df[df['ltp'].diff() > threshhold]['game_time'].values.tolist()
    if jump_times:
        return jump_times
    else:
        return None


def backtest_back_lay(back_stake, odds_df, starting_bankroll):
    bankroll = starting_bankroll
    bankroll_record = []
    for series_uid in odds_df['series_uid'].unique():
        bankroll_record.append(bankroll)
        sub_df = odds_df[odds_df['series_uid'] == series_uid]
        jump_times = get_minute_of_price_jumps(sub_df, 2)
        if jump_times is None:
            continue
        if len(jump_times) < 2:
            continue
        back_time = jump_times[1] + 1
        print(back_time)
        lay_time = back_time + 10
        if lay_time not in sub_df['game_time'].values.tolist():
            profit = -back_stake
        else:
            back_odds = sub_df[sub_df['game_time'] == back_time]['ltp'].values[0]
            lay_odds = sub_df[sub_df['game_time'] == lay_time]['ltp'].values[0]
            lay_stake = calc.equal_hedge(back_odds=back_odds, lay_odds=lay_odds,
                                         back_stake=back_stake)
            profit = calc.profit(back_odds=back_odds, lay_odds=lay_odds,
                                 back_stake=back_stake, lay_stake=lay_stake)[0]
        if profit > 0:
            profit = 0.95 * profit
        bankroll = bankroll + profit
        print(profit)
        print(bankroll)

    return bankroll_record


df = get_odds_data(runner=RCEnum.UNDER_2_5_GOALS, divisions=[DivisionCodeEnum.PREMIER_LEAGUE,
                                                             DivisionCodeEnum.CHAMPIONSHIP],
                   in_play=True, item_freq_type_code=IFTCEnum.MINUTE)
bankroll_record_ = backtest_back_lay(100, odds_df=df, starting_bankroll=1000)
plt.plot(bankroll_record_)
plt.show()

