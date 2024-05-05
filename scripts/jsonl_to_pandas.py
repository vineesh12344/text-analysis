import json
import pandas as pd


lines = []
with open('result.jsonl') as f:
    lines = f.read().splitlines()

line_dicts = [json.loads(line) for line in lines]
df_final = pd.DataFrame(line_dicts)

df_final.to_csv('test.csv', index=False)