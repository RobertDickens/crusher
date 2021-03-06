from orm.orm import ItemFreqType
from utils.db.database_manager import dbm
from utils.custom_enum import CustomEnum


class ItemFreqTypeCodeEnum(CustomEnum):
    MINUTE = 'MINUTE'
    SECOND = 'SECOND'


def create_or_update_item_freq_type():
    with dbm.get_managed_session() as session:
        ItemFreqType.create_or_update(session, item_freq_type_code=ItemFreqTypeCodeEnum.MINUTE,
                                      item_freq_type_desc='minute')
        ItemFreqType.create_or_update(session, item_freq_type_code=ItemFreqTypeCodeEnum.SECOND,
                                      item_freq_type_desc='second')


if __name__ == '__main__':
    create_or_update_item_freq_type()
