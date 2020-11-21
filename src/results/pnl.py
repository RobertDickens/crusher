import os
import pandas as pd

from datetime import time
from matplotlib import pyplot as plt

pl_path = 'C:\\Users\\rober\\AppData\\Roaming\\Bet Angel\\Bet Angel Professional\\AutomationReports'

df = pd.DataFrame(columns=[' Market', ' Profit If Win'])
for subdir, dirs, files in os.walk(pl_path):
    for file in files:
        day_df = pd.read_csv(os.path.join(pl_path, file))
        day_df['Time'] = day_df['Time'].apply(lambda x: time.fromisoformat(x))
        day_df = day_df.sort_values('Time', ascending=True)
        day_df = day_df.drop(['Time', ' Selection', ' Profit If Lose'], axis=1)
        df = df.append(day_df)

df = df.groupby(' Market').max()
df[' Profit If Win'].expanding().sum().plot()
plt.show()
