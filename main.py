
import pandas as pd
from sqlalchemy import create_engine



# Чтение данных
df = pd.read_csv(r"C:\Users\EvOlZ\.cache\kagglehub\datasets\antonbelyaevd\yandex-music-top-100-songs\versions\1\yandex_tracks_top100.csv")

print(df.columns)
