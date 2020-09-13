from utils.helper_functions.data_loading import get_odds_data
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.division import DivisionCodeEnum as DCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from utils.helper_functions import calculation as calc
from matplotlib import pyplot as plt


df = get_odds_data(RCEnum.SCORE_0_0,
                   divisions=[DCEnum.CHAMPIONSHIP],
                   item_freq_type_code=IFTCEnum.MINUTE,
                   in_play=True)


def backtest_back_lay(back_stake, odds_df, lay_time, back_time, starting_bankroll):
    bankroll = starting_bankroll
    bankroll_record = []
    for series_uid in odds_df['series_uid'].unique():
        bankroll_record.append(bankroll)
        sub_df = odds_df[odds_df['series_uid'] == series_uid]
        back_odds = sub_df[sub_df['game_time'] == back_time]['ltp'].values[0]
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

    return bankroll_record


bankroll_record_ = backtest_back_lay(100, odds_df=df, back_time=0, lay_time=15,
                                     starting_bankroll=1000)
plt.plot(bankroll_record_)
plt.show()
