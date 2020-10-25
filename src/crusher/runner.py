from orm.orm import Runner
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class RunnerCodeEnum(CustomEnum):
    SCORE_0_0 = 'SCORE_0_0'
    SCORE_1_0 = 'SCORE_1_0'
    SCORE_2_0 = 'SCORE_2_0'
    SCORE_3_0 = 'SCORE_3_0'
    SCORE_0_1 = 'SCORE_0_1'
    SCORE_1_1 = 'SCORE_1_1'
    SCORE_2_1 = 'SCORE_2_1'
    SCORE_3_1 = 'SCORE_3_1'
    SCORE_0_2 = 'SCORE_0_2'
    SCORE_1_2 = 'SCORE_1_2'
    SCORE_2_2 = 'SCORE_2_2'
    SCORE_3_2 = 'SCORE_3_2'
    SCORE_0_3 = 'SCORE_0_3'
    SCORE_1_3 = 'SCORE_1_3'
    SCORE_2_3 = 'SCORE_2_3'
    SCORE_3_3 = 'SCORE_3_3'
    ANY_OTHER_HOME_WIN = 'ANY_OTHER_HOME_WIN'
    ANY_OTHER_AWAY_WIN = 'ANY_OTHER_AWAY_WIN'
    ANY_OTHER_DRAW = 'ANY_OTHER_DRAW'
    UNDER_1_5_GOALS = 'UNDER_1_5_GOALS'
    UNDER_2_5_GOALS = 'UNDER_2_5_GOALS'
    UNDER_3_5_GOALS = 'UNDER_3_5_GOALS'
    UNDER_4_5_GOALS = 'UNDER_4_5_GOALS'
    UNDER_5_5_GOALS = 'UNDER_5_5_GOALS'
    OVER_1_5_GOALS = 'OVER_1_5_GOALS'
    OVER_2_5_GOALS = 'OVER_2_5_GOALS'
    OVER_3_5_GOALS = 'OVER_3_5_GOALS'
    OVER_4_5_GOALS = 'OVER_4_5_GOALS'
    OVER_5_5_GOALS = 'OVER_5_5_GOALS'
    TEAM_A = 'TEAM_A'
    TEAM_B = 'TEAM_B'
    THE_DRAW = 'THE_DRAW'
    FAVOURITE = 'FAVOURITE'
    SECOND_FAVOURITE = 'SECOND_FAVOURITE'
    THIRD_FAVOURITE = 'THIRD_FAVOURITE'
    FOURTH_FAVOURITE = 'FOURTH_FAVOURITE'
    FIFTH_FAVOURITE = 'FIFTH_FAVOURITE'
    SIXTH_FAVOURITE = 'SIXTH_FAVOURITE'
    SEVENTH_FAVOURITE = 'SEVENTH_FAVOURITE'
    EIGTH_FAVOURTE = 'EIGTH_FAVOURTE'
    NINTH_FAVOURITE = 'NINTH_FAVOURITE'
    TENTH_FAVOURITE = 'TENTH_FAVOURITE'
    ELEVENTH_FAVOURITE = 'ELEVENTH_FAVOURITE'
    TWELFTH_FAVOURITE = 'TWELFTH_FAVOURITE'
    THIRTEENTH_FAVOURITE = 'THIRTEENTH_FAVOURITE'
    FOURTEENTH_FAVOURITE = 'FOURTEENTH_FAVOURITE'


horse_racing_runner_map = {1: RunnerCodeEnum.FAVOURITE,
                           2: RunnerCodeEnum.SECOND_FAVOURITE,
                           3: RunnerCodeEnum.THIRD_FAVOURITE,
                           4: RunnerCodeEnum.FOURTH_FAVOURITE,
                           5: RunnerCodeEnum.FIFTH_FAVOURITE,
                           6: RunnerCodeEnum.SIXTH_FAVOURITE,
                           7: RunnerCodeEnum.SEVENTH_FAVOURITE,
                           8: RunnerCodeEnum.EIGTH_FAVOURTE,
                           9: RunnerCodeEnum.NINTH_FAVOURITE,
                           10: RunnerCodeEnum.TENTH_FAVOURITE,
                           11: RunnerCodeEnum.ELEVENTH_FAVOURITE,
                           12: RunnerCodeEnum.TWELFTH_FAVOURITE,
                           13: RunnerCodeEnum.THIRTEENTH_FAVOURITE,
                           14: RunnerCodeEnum.FOURTEENTH_FAVOURITE}

football_runner_betfair_map = {'0 - 0': RunnerCodeEnum.SCORE_0_0,
                               '1 - 0': RunnerCodeEnum.SCORE_1_0,
                               '2 - 0': RunnerCodeEnum.SCORE_2_0,
                               '3 - 0': RunnerCodeEnum.SCORE_3_0,
                               '0 - 1': RunnerCodeEnum.SCORE_0_1,
                               '1 - 1': RunnerCodeEnum.SCORE_1_1,
                               '2 - 1': RunnerCodeEnum.SCORE_2_1,
                               '3 - 1': RunnerCodeEnum.SCORE_3_1,
                               '0 - 2': RunnerCodeEnum.SCORE_0_2,
                               '1 - 2': RunnerCodeEnum.SCORE_1_2,
                               '2 - 2': RunnerCodeEnum.SCORE_2_2,
                               '3 - 2': RunnerCodeEnum.SCORE_3_2,
                               '0 - 3': RunnerCodeEnum.SCORE_0_3,
                               '1 - 3': RunnerCodeEnum.SCORE_1_3,
                               '2 - 3': RunnerCodeEnum.SCORE_2_3,
                               '3 - 3': RunnerCodeEnum.SCORE_3_3,
                               'Any Other Home Win': RunnerCodeEnum.ANY_OTHER_HOME_WIN,
                               'Any Other Away Win': RunnerCodeEnum.ANY_OTHER_AWAY_WIN,
                               'Any Other Draw': RunnerCodeEnum.ANY_OTHER_DRAW,
                               'Under 1.5 Goals': RunnerCodeEnum.UNDER_1_5_GOALS,
                               'Under 2.5 Goals': RunnerCodeEnum.UNDER_2_5_GOALS,
                               'Under 3.5 Goals': RunnerCodeEnum.UNDER_3_5_GOALS,
                               'Under 4.5 Goals': RunnerCodeEnum.UNDER_4_5_GOALS,
                               'Under 5.5 Goals': RunnerCodeEnum.UNDER_5_5_GOALS,
                               'Over 1.5 Goals': RunnerCodeEnum.OVER_1_5_GOALS,
                               'Over 2.5 Goals': RunnerCodeEnum.OVER_2_5_GOALS,
                               'Over 3.5 Goals': RunnerCodeEnum.OVER_3_5_GOALS,
                               'Over 4.5 Goals': RunnerCodeEnum.OVER_4_5_GOALS,
                               'Over 5.5 Goals': RunnerCodeEnum.OVER_5_5_GOALS,
                               '1': RunnerCodeEnum.FAVOURITE,
                               '2': RunnerCodeEnum.SECOND_FAVOURITE,
                               '3': RunnerCodeEnum.THIRD_FAVOURITE,
                               '4': RunnerCodeEnum.FOURTH_FAVOURITE,
                               '5': RunnerCodeEnum.FIFTH_FAVOURITE,
                               '6': RunnerCodeEnum.SIXTH_FAVOURITE,
                               '7': RunnerCodeEnum.SEVENTH_FAVOURITE,
                               '8': RunnerCodeEnum.EIGTH_FAVOURTE,
                               '9': RunnerCodeEnum.NINTH_FAVOURITE,
                               '10': RunnerCodeEnum.TENTH_FAVOURITE,
                               '11': RunnerCodeEnum.ELEVENTH_FAVOURITE,
                               '12': RunnerCodeEnum.TWELFTH_FAVOURITE,
                               '13': RunnerCodeEnum.THIRTEENTH_FAVOURITE,
                               '14': RunnerCodeEnum.FOURTEENTH_FAVOURITE,
                               }

inverse_runner_betfair_map = {v: k for k, v in football_runner_betfair_map.items()}


def create_or_update_runners():
    with dbm.get_managed_session() as session:
        for runner_code in RunnerCodeEnum.to_dict().values():
            Runner.create_or_update(session, runner_code=runner_code,
                                    runner_betfair_code=inverse_runner_betfair_map.get(runner_code))


if __name__ == '__main__':
    create_or_update_runners()
