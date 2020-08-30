from orm.orm import Division
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class DivisionCodeEnum(CustomEnum):
    PREMIER_LEAGUE = 'PREMIER_LEAGUE'
    CHAMPIONSHIP = 'CHAMPIONSHIP'
    LEAGUE_1 = 'LEAGUE_1'
    LEAGUE_2 = 'LEAGUE_2'
    CONFERENCE = 'CONFERENCE'


def create_or_update_market_type():
    with dbm.get_managed_session() as session:
        Division.create_or_update(session, division_code=DivisionCodeEnum.PREMIER_LEAGUE,
                                  division_name='premier league')
        Division.create_or_update(session, division_code=DivisionCodeEnum.CHAMPIONSHIP,
                                  division_name='championship')
        Division.create_or_update(session, division_code=DivisionCodeEnum.LEAGUE_1,
                                  division_name='league 1')
        Division.create_or_update(session, division_code=DivisionCodeEnum.LEAGUE_2,
                                  division_name='league 2')
        Division.create_or_update(session, division_code=DivisionCodeEnum.CONFERENCE,
                                  division_name='conference')


if __name__ == '__main__':
    create_or_update_market_type()
