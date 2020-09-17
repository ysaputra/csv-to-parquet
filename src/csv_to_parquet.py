import pandas as pd

csvPath = "../data/sample_data.csv"
parquetFilename = "../output/csvParquet.parquet"

df = pd.read_csv(csvPath)
df.to_parquet(path=parquetFilename, compression='GZIP')
print(pd.read_parquet(parquetFilename))
