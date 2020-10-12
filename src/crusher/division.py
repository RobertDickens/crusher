from orm.orm import Division
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class DivisionCodeEnum(CustomEnum):
    PREMIER_LEAGUE = 'PREMIER_LEAGUE'
    CHAMPIONSHIP = 'CHAMPIONSHIP'
    LEAGUE_1 = 'LEAGUE_1'
    LEAGUE_2 = 'LEAGUE_2'
    CONFERENCE = 'CONFERENCE'
    FRANCE_LEAGUE_1 = 'FRANCE_LEAGUE_1'
    GERMANY_BUNDESLIGA = 'GERMANY_BUNDESLIGA'
    GERMANY_BUNDESLIGA_2 = 'GERMANY_BUDESLIGA_2'
    ITALY_SERIE_A = 'ITALY_SERIE_A'
    ITALY_SERIE_B = 'ITALY_SERIE_B'
    SPAIN_LA_LIGA = 'SPAIN_LA_LIGA'
    SPAIN_SEGUNDA = 'SPAIN_SEGUNDA'
    UEFA_CHAMPIONS_LEAGUE = 'UEFA_CHAMPIONS_LEAGUE'
    UEFA_EUROPA_LEAGUE = 'UEFA_EUROPA_LEAGUE'


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
        Division.create_or_update(session, division_code=DivisionCodeEnum.FRANCE_LEAGUE_1,
                                  division_name='france league 1')
        Division.create_or_update(session, division_code=DivisionCodeEnum.GERMANY_BUNDESLIGA,
                                  division_name='german bundesliga')
        Division.create_or_update(session, division_code=DivisionCodeEnum.GERMANY_BUNDESLIGA_2,
                                  division_name='germany bundesliga 2')
        Division.create_or_update(session, division_code=DivisionCodeEnum.ITALY_SERIE_A,
                                  division_name='italy serie a')
        Division.create_or_update(session, division_code=DivisionCodeEnum.ITALY_SERIE_B,
                                  division_name='italy series b')
        Division.create_or_update(session, division_code=DivisionCodeEnum.SPAIN_LA_LIGA,
                                  division_name='spain la liga')
        Division.create_or_update(session, division_code=DivisionCodeEnum.SPAIN_SEGUNDA,
                                  division_name='spain segunda')
        Division.create_or_update(session, division_code=DivisionCodeEnum.UEFA_CHAMPIONS_LEAGUE,
                                  division_name='uefa champions league')
        Division.create_or_update(session, division_code=DivisionCodeEnum.UEFA_EUROPA_LEAGUE,
                                  division_name='uefa europa league')


if __name__ == '__main__':
    create_or_update_market_type()
