import os
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def create_bigquery_dataset(dataset_id: str):
    # Set up a BigQuery client object
    client = bigquery.Client()

    # Create the dataset object
    dataset = bigquery.Dataset(dataset_id)

    # Check if the dataset already exists
    try:
        client.get_dataset(dataset_id)  # Make an API request.
        logging.info(f"Dataset {dataset_id} already exists")
    except Exception:
        # Set additional properties for the dataset
        dataset.location = 'EU'
        
        # Create the dataset if it does not exist
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        logging.info(f"Created dataset {dataset.dataset_id} in project {dataset.project}.")
    except Exception as e:
        logging.error(f"Failed to create dataset {dataset_id}: {str(e)}")


def load_parquet_to_bigquery(year: int, month: int, base_link: str, dataset_id: str):
    # Set up a BigQuery client object
    client = bigquery.Client()

    # Check if the dataset exists
    create_bigquery_dataset(dataset_id)

    # Defining the path to the file
    file_name = f'yellow_tripdata_{year}-{month}.parquet'
    file_name_out = f'yellow_tripdata_{year}_{month}'
    table_id = f'{dataset_id}.{file_name_out}'
    parquet_path = os.path.join(base_link, file_name)

    # Read the Parquet file into a pandas DataFrame
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        logging.error(f"Failed to load Parquet file: {str(e)}")
        return

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
    dataset_id = os.environ.get("DATASET_ID", "kevin-task.yellow_taxi_raw")

    # Load the Parquet file into BigQuery
    load_parquet_to_bigquery(year, month, base_link, dataset_id)


if __name__ == "__main__":
    main()
