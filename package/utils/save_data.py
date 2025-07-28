import pandas as pd
import os

def save_data_to_csv(df : pd.DataFrame, filename : str):
    if os.path.exists(filename) and os.path.getsize(filename) != 0:
        df.to_csv(filename, index=False, header=False, encoding='utf-8', mode='a')
    else :
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, header=True, encoding='utf-8')