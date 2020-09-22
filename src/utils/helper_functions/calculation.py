import numpy as np


def profit_percentage_from_odds_diff(odds_diff):
    """Calculates profit as a percentage of backing stake
    from odds difference"""
    return odds_diff - 1


def equal_hedge(back_odds, lay_odds, back_stake=None, lay_stake=None):
    if back_stake and lay_stake:
        raise ValueError("Must provide either back stake or lay stake, not both")
    if back_stake:
        return (back_odds * back_stake) / lay_odds
    elif lay_stake:
        return (lay_odds * lay_stake) / back_odds
    else:
        raise ValueError("Provide back or lay stake")


def profit(back_stake, lay_stake, back_odds, lay_odds):
    """Calculate back profit and lay profit"""
    back_profit = (back_stake * back_odds) - (lay_stake * lay_odds) - back_stake + lay_stake
    lay_profit = lay_stake - back_stake
    return back_profit, lay_profit


def green(back_odds=None, lay_odds=None, back_stake=None, lay_stake=None):
    if back_stake and lay_stake:
        raise ValueError("Must provide either back stake or lay stake, not both")
    if back_stake:
        lay_stake = equal_hedge(back_odds, float(lay_odds), back_stake=back_stake)

        profit_ = profit(back_stake=back_stake,
                         lay_stake=lay_stake,
                         back_odds=back_odds,
                         lay_odds=float(lay_odds))[0]
    elif lay_stake:
        back_stake = equal_hedge(back_odds, float(lay_odds), lay_stake=lay_stake)
        profit_ = profit(back_stake=back_stake,
                         lay_stake=lay_stake,
                         back_odds=back_odds,
                         lay_odds=float(lay_odds))[0]
    else:
        raise ValueError("Need either back_stake or lay_stake")

    # Apply commission
    if profit_ > 0:
        profit_ = 0.95 * profit_

    return profit_
