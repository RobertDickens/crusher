from orm.orm import Sport
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class SportCodeEnum(CustomEnum):
    FOOTBALL = 'FOOTBALL'
    HORSE_RACING = 'HORSE_RACING'


def create_or_update_sport():
    with dbm.get_managed_session() as session:
        Sport.create_or_update(session, sport_code=SportCodeEnum.FOOTBALL,
                               sport_name='football')
        Sport.create_or_update(session, sport_code=SportCodeEnum.HORSE_RACING,
                               sport_name='horse racing')


if __name__ == '__main__':
    create_or_update_sport()
