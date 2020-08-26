import string

from orm.orm import Country
from utils.database_manager import dbm

import pycountry

skip_countries = ['Åland Islands']

mapping = {c.name: c.alpha_2 for c in pycountry.countries if c.name not in skip_countries}
with dbm.get_managed_session() as session:
    for country_name, country_code in mapping.items():
        country_name = country_name.lower()
        country_name = country_name.translate(str.maketrans('', '', string.punctuation))
        Country.create_or_update(session, country_name=country_name,
                                 country_code=country_code)
