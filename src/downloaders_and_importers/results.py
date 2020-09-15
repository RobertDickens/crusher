import os
import string
from datetime import datetime

from utils.db.database_manager import dbm
from utils.parsers.football_data_dot_com_parser import parse_football_data_com
from orm.orm import Event, Team, Result

import pandas as pd

str_replacements = {'sheffield weds': 'sheff wed',
                    'sheffield united': 'sheff utd',
                    'crystal palace': 'c palace',
                    'havant  waterlooville': 'havant and w',
                    'skipping braintree town': 'braintree',
                    'braintree town': 'braintree',
                    'harrogate': 'harrogate town',
                    'dover athletic': 'dover',
                    'fylde': 'afc fylde',
                    'solihull': 'solihull moors',
                    'maidstone': 'maidstone utd',
                    'salford': 'salford city',
                    'sutton': 'sutton utd',
                    'milton keynes dons': 'mk dons',
                    'notts county': 'notts co',
                    'cambridge': 'cambridge utd',
                    'oxford': 'oxford utd',
                    'bristol rvs': 'bristol rovers',
                    'peterboro': 'peterborough',
                    'halifax': 'fc halifax town',
                    'man united': 'man utd'}


root_dir = r'C:\Users\rober\sport_data\CSV\football_data_com'
with dbm.get_managed_session() as session:
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            print(file)
            raw_df = pd.read_csv(os.path.join(subdir, file), encoding="ISO-8859-1")
            df = parse_football_data_com(raw_df)
            for index, row in df.iterrows():
                team_a_name = row['home_team'].lower().translate(str.maketrans('', '', string.punctuation))
                team_b_name = row['away_team'].lower().translate(str.maketrans('', '', string.punctuation))
                for original_str, replacement_str in str_replacements.items():
                    team_a_name = team_a_name.replace(original_str, replacement_str)
                    team_b_name = team_b_name.replace(original_str, replacement_str)
                try:
                    team_a = Team.get_by_team_name(session, team_a_name)
                    team_b = Team.get_by_team_name(session, team_b_name)
                except:
                    print(f'skipping {team_a_name} v {team_b_name}, could not find one or both teams')
                    continue

                try:
                    event = Event.get_by_teams_and_date(session, team_a, team_b, row['match_date'])
                    result = Result.create_or_update(session,
                                                     event=event,
                                                     team_a_goals=row['full_time_home_goals'],
                                                     team_b_goals=row['full_time_away_goals'],
                                                     update_datetime=datetime.utcnow(),
                                                     creation_datetime=datetime.utcnow())
                    event.division_code = row['division']
                    print(row['division'])
                except:
                    print(f"skipping {team_a_name} v {team_b_name}, {row['match_date']} could not find event")
