import pandas as pd
import os


filename = 'premier_league_total_2019.csv'


def parse_data_from_raw_excel(csv_file_path):
    df = pd.read_csv(os.path.join(raw_data_path, csv_file_path))
    df = df.drop(['1st H.', '2nd H.'], axis=1)
    target_cols = [col for col in df.columns if col != 'Team']
    df[target_cols] = df[target_cols].applymap(lambda s: s.split('-'))

    df_scored = df.copy(deep=True)
    df_conceded = df.copy(deep=True)
    df_total = df.copy(deep=True)

    df_scored[target_cols] = df[target_cols].applymap(lambda l: int(l[0]))
    df_conceded[target_cols] = df[target_cols].applymap(lambda l: int(l[1]))
    df_total[target_cols] = df[target_cols].applymap(lambda l: int(l[0]) + int(l[1]))

    return df_scored, df_conceded, df_conceded

x, y, z = parse_data_from_raw_excel(os.path.join(raw_data_path, filename))
print(x)