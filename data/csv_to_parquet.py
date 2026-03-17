import pandas as pd

input_path = "loan_approval_dataset.csv"
output_path = "loan_approval.parquet"

df = pd.read_csv(input_path)

df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print(df.dtypes)
print(df.head(3))

df.to_parquet(output_path, index=False)
print(f"\nSaved to {output_path}")
