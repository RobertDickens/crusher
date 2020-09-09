import numpy as np


def calculate_profit(back_stake, lay_stake, back_odds, lay_odds):
    """Calculate back profit and lay profit"""
    back_profit = (back_stake * back_odds) - (lay_stake * lay_odds) - back_stake + lay_stake
    lay_profit = lay_stake - back_stake
    return back_profit, lay_profit


def calculate_profit_as_percentage_of_back_stake(back_odds, lay_odds):
    """Calculates profit as a percentage of backing stake
    based on an even hedging strategy"""
    back_stake = 100
    lay_stake = (back_stake * back_odds) / lay_odds
    profit = calculate_profit(back_stake, lay_stake, back_odds, lay_odds)
    return profit

back_odds = np.arange(2, 100)
lay_odds = back_odds-1
for b, l in zip(back_odds, lay_odds):
    profit = calculate_profit_as_percentage_of_back_stake(b, l)
    print(profit)