import requests
import random
import pandas as pd


df_club = pd.read_csv("data/clubs.csv")

df_club["id"] = df_club["url"].map(lambda x: int(x.split("/")[-1]))



cols =["id", "name", "url"]
df_club = df_club[cols]
df_club.sort_values(by="id", ascending=[True], inplace=True)
print(df_club.head())

df_club.to_csv("data/clubs_cleaned.csv", index=False)
