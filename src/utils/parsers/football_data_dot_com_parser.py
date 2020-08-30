import pandas as pd
import numpy as np

from crusher.division import DivisionCodeEnum


def parse_football_data_com(df):
    # rename columns
    column_renames = {'Div': 'division',
                      'Date': 'match_date',
                      'HomeTeam': 'home_team',
                      'AwayTeam': 'away_team',
                      'FTHG': 'full_time_home_goals',
                      'FTAG': 'full_time_away_goals',
                      'FTR': 'full_time_result',
                      'HTHG': 'half_time_home_team_goals',
                      'HTAG': 'half_time_away_team_goals',
                      'HTR': 'half_time_result',
                      'Referee': 'referee'}
    df = df.rename(columns=column_renames)
    df = df[[col for col in column_renames.values()]]

    division_map = {'E0': DivisionCodeEnum.PREMIER_LEAGUE,
                    'E1': DivisionCodeEnum.CHAMPIONSHIP,
                    'E2': DivisionCodeEnum.LEAGUE_1,
                    'E3': DivisionCodeEnum.LEAGUE_2,
                    'EC': DivisionCodeEnum.CONFERENCE}
    df['division'] = df['division'].apply(lambda d: division_map[d])
    df['match_date'] = pd.to_datetime(df['match_date'])

    return df
