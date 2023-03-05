import pandas as pd
from google.cloud import bigquery

# Set up a BigQuery client object
client = bigquery.Client()

# Defining the path to the file
year, month = 2019, 12
file_name = 'yellow_tripdata_{}-{}.parquet'.format(year, month)
file_name_out = 'yellow_tripdata_{}_{}'.format(year, month)
base_link = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
parquet_path = base_link + file_name

# Read the Parquet file into a pandas DataFrame
df = pd.read_parquet(parquet_path)

print(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns for transfer.")

# Define the BigQuery table ID (in the format 'project_id.dataset_id.table_id')
table_id = 'kevin-task.yellow_taxi_raw.' + file_name_out

# Write the DataFrame to BigQuery
job_config = bigquery.LoadJobConfig(autodetect=True)
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete

print(f"Loaded {job.output_rows} rows into BigQuery table {table_id}.")
