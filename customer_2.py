from google.cloud import bigquery
from google.cloud import storage
import json
import os


credentials_path = 'C:/Users/Malindu/Desktop/work/customer-356610-934e9553ed93.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]  = credentials_path

from google.oauth2 import service_account
from dotenv import load_dotenv
load_dotenv()

credentials = service_account.Credentials.from_service_account_file("C:/Users/Malindu/Desktop/work/customer-356610-934e9553ed93.json")


client = bigquery.Client()
# filename = 'C:/Users/Malindu/Desktop/work/customers_2.json'
dataset_id = 'customer-356610.cus'
table_id = 'customer-356610.cus.customer_1'

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()


job_config = bigquery.LoadJobConfig(
    autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)
uri = 'gs://customer_task/customers_2.json'
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.
load_job.result()  # Waits for the job to complete.
destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))
















