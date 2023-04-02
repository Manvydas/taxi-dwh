import os
import logging
import pandas as pd
from google.cloud import bigquery


def load_parquet_to_bigquery(year: int, month: int, base_link: str, table_id: str):
    # Set up a BigQuery client object
    client = bigquery.Client()

    # Defining the path to the file
    file_name = f'yellow_tripdata_{year}-{month}.parquet'
    file_name_out = f'yellow_tripdata_{year}_{month}'
    parquet_path = os.path.join(base_link, file_name)

    # Read the Parquet file into a pandas DataFrame
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        logging.error(f"Failed to load Parquet file: {str(e)}")
        return

    # Define the BigQuery table ID (in the format 'project_id.dataset_id.table_id')
    table_id = f'{table_id}.{file_name_out}'

    # Write the DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(autodetect=True)
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
    except Exception as e:
        logging.error(f"Failed to write DataFrame to BigQuery: {str(e)}")
        return

    logging.info(f"Loaded {job.output_rows} rows into BigQuery table {table_id}.")


def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Load environment variables
    year = int(os.environ.get("YEAR", 2019))
    month = int(os.environ.get("MONTH", 12))
    base_link = os.environ.get("BASE_LINK", "https://d37ci6vzurychx.cloudfront.net/trip-data/")
    table_id = os.environ.get("TABLE_ID", "kevin-task.yellow_taxi_raw")

    # Load the Parquet file into BigQuery
    load_parquet_to_bigquery(year, month, base_link, table_id)


if __name__ == "__main__":
    main()
