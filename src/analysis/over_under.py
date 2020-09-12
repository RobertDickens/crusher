from utils.db.database_manager import dbm
from orm.orm import ExchangeOddsSeriesItem, Runner, ExchangeOddsSeries
from crusher.runner import RunnerCodeEnum as RCEnum
from crusher.item_freq_type import ItemFreqTypeCodeEnum as IFTCEnum
from crusher.division import DivisionCodeEnum
from utils.helper_functions import preprocessing as pr
from utils.helper_functions import calculation as calc
from matplotlib import pyplot as plt

# Modelling P/L from under 2.5 goals
with dbm.get_managed_session() as session:
    # Get data
    runner = Runner.get_by_code(session, RCEnum.UNDER_2_5_GOALS)
    df = ExchangeOddsSeriesItem.get_series_items_df(session,
                                                    division_code=[DivisionCodeEnum.PREMIER_LEAGUE,
                                                                   DivisionCodeEnum.CHAMPIONSHIP,
                                                                   ],
                                                    runner_uid=runner.runner_uid,
                                                    in_play=True,
                                                    item_freq_type_code=IFTCEnum.MINUTE
                                                    )
df = df.sort_values(['series_uid', 'published_datetime'], ascending=True)
df = pr.convert_published_datetime_to_game_time(df)
df = df[['series_uid', 'game_time', 'ltp']]

bankroll = 100
bankroll_record = []
for series_uid in df['series_uid'].unique():
    bankroll_record.append(bankroll)
    sub_df = df[df['series_uid'] == series_uid]
    back_odds = sub_df[sub_df['game_time'] == 0]['ltp'].values[0]
    try:
        lay_odds = sub_df[sub_df['game_time'] == 30]['ltp'].values[0]
        lay_stake = calc.equal_hedge(back_odds=back_odds, lay_odds=lay_odds, back_stake=10)
        profit = calc.profit(back_odds=back_odds, lay_odds=lay_odds,
                             back_stake=10, lay_stake=lay_stake)[0]
    except:
        profit = -10
    if profit > 0:
        profit = 0.95 * profit
    bankroll = bankroll + profit

print(bankroll)
plt.plot(range(0, len(bankroll_record)), bankroll_record)
plt.show()

