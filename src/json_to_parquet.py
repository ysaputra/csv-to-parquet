import pandas as pd
import json

jsonPath = "../data/6190.json"
parquetFilename = "../output/jsonParquet.parquet"

with open(jsonPath, 'r') as f:
    data = json.loads(f.read())

df = pd.json_normalize(data)
df.to_parquet(path=parquetFilename, compression='GZIP')
print(pd.read_parquet(parquetFilename))
