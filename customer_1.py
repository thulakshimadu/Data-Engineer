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
filename = 'C:/Users/Malindu/Desktop/work/customers_1.json'
dataset_id = 'customer-356610.cus'
table_id = 'customer-356610.cus.customer_1'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True

with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # API request

job.result()  # Waits for table load to complete.

print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))