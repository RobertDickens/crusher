from orm.orm import ItemFreqType
from utils.database_manager import dbm
from utils.custom_enum import CustomEnum


class ItemFreqTypeCodeEnum(CustomEnum):
    MINUTE = 'MINUTE'
    SECOND = 'SECOND'


if __name__ == '__main__':
    with dbm.get_managed_session() as session:
        session.add(ItemFreqType(item_freq_type_code=ItemFreqTypeCodeEnum.MINUTE,
                                 item_freq_type_desc='minute'))
        session.add(ItemFreqType(item_freq_type_code=ItemFreqTypeCodeEnum.SECOND,
                                 item_freq_type_desc='second'))
