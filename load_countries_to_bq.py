import pandas as pd
from google.cloud import bigquery

# Set up a BigQuery client object
client = bigquery.Client()

# Defining the path to the file
file_name_out = 'taxi_zones_lookup'
file_path = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'

# Read the Parquet file into a pandas DataFrame
df = pd.read_csv(file_path)

data_dim = df.shape
print(f"Loaded {data_dim[0]} rows and {data_dim[1]} columns for transfer.")

# Define the BigQuery table ID (in the format 'project_id.dataset_id.table_id')
table_id = 'turing-example.yellow_taxi_raw.' + file_name_out

# Write the DataFrame to BigQuery
job_config = bigquery.LoadJobConfig(autodetect=True)
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete

print(f"Loaded {job.output_rows} rows into BigQuery table {table_id}.")
