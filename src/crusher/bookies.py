from orm.orm import Bookie
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class BookieNameEnum(CustomEnum):
    MARATHON_BET = 'MARATHON_BET'
    PADDY_POWER = 'PADDY_POWER'
    BET_VICTOR = 'BET_VICTOR'
    XBET_1 = '1_XBET'
    UNIBET = 'UNIBET'
    SPORT_888 = '888SPORT'
    BETWAY = 'BETWAY'
    SKY_BET = 'SKYBET'
    BETCLIC = 'BETCLIC'
    LADBROKES = 'LADBROKES'
    WILLIAM_HILL = 'WILLIAM_HILL'


def create_or_update_bookies():
    with dbm.get_managed_session() as session:
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.MARATHON_BET,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.PADDY_POWER,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.BET_VICTOR,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.XBET_1,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.UNIBET,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.SPORT_888,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.BETWAY,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.SKY_BET,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.BETCLIC,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.LADBROKES,
                                bookie_name='marathon bet')
        Bookie.create_or_update(session,
                                bookie_code=BookieNameEnum.WILLIAM_HILL,
                                bookie_name='marathon bet')


if __name__ == '__main__':
    create_or_update_bookies()
