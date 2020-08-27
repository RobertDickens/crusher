from orm.orm import Runner
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class RunnerCode(CustomEnum):
    SCORE_0_0 = 'SCORE_0_0'
    SCORE_1_0 = 'SCORE_1_0'
    SCORE_2_0 = 'SCORE_2_0'
    SCORE_3_0 = 'SCORE_3_0'
    SCORE_0_1 = 'SCORE_0_1'
    SCORE_1_1 = 'SCORE_2_1'
    SCORE_2_1 = 'SCORE_3_1'
    SCORE_3_1 = 'SCORE_4_1'
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


runner_betfair_map = {'0 - 0': RunnerCode.SCORE_0_0,
                      '1 - 0': RunnerCode.SCORE_1_0,
                      '2 - 0': RunnerCode.SCORE_2_0,
                      '3 - 0': RunnerCode.SCORE_3_0,
                      '0 - 1': RunnerCode.SCORE_0_1,
                      '1 - 1': RunnerCode.SCORE_1_1,
                      '2 - 1': RunnerCode.SCORE_2_1,
                      '3 - 1': RunnerCode.SCORE_3_1,
                      '0 - 2': RunnerCode.SCORE_0_2,
                      '1 - 2': RunnerCode.SCORE_1_2,
                      '2 - 2': RunnerCode.SCORE_2_2,
                      '3 - 2': RunnerCode.SCORE_3_2,
                      '0 - 3': RunnerCode.SCORE_0_3,
                      '1 - 3': RunnerCode.SCORE_1_3,
                      '2 - 3': RunnerCode.SCORE_2_3,
                      '3 - 3': RunnerCode.SCORE_3_3,
                      'Any Other Home Win': RunnerCode.ANY_OTHER_HOME_WIN,
                      'Any Other Away Win': RunnerCode.ANY_OTHER_AWAY_WIN,
                      'Any Other Draw': RunnerCode.ANY_OTHER_DRAW}

inverse_runner_betfair_map = {v: k for k, v in runner_betfair_map.items()}

if __name__ == '__main__':
    with dbm.get_managed_session() as session:
        for runner_code in RunnerCode.to_dict().values():
            Runner.create_or_update(session, runner_code=runner_code,
                                    runner_betfair_code=inverse_runner_betfair_map[runner_code])
