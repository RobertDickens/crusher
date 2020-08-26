from orm.orm import InfoSourceOrganisation
from utils.database_manager import dbm
from utils.custom_enum import CustomEnum


class InfoSourceOrganisationEnum(CustomEnum):
    BETFAIR = 'BETFAIR'


if __name__ == '__main__':
    with dbm.get_managed_session() as session:
        session.add(InfoSourceOrganisation(orgn_name=InfoSourceOrganisationEnum.BETFAIR,
                                           organisation_url='www.betfair.co.uk'))
