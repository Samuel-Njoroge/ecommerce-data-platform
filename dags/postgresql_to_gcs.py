from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'postgresql_to_gcs',
    default_args=default_args,
    description='Transfer data from PostgreSQL to Google Cloud Storage',
    schedule_interval='@daily',
)

def transfer_postgresql_to_gcs(table_name, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # Extract data from PostgreSQL
    df = postgres_hook.get_pandas_df(f"SELECT * FROM {table_name}")
    
    # Convert DataFrame to CSV
    csv_data = df.to_csv(index=False)
    
    # Upload CSV to GCS
    bucket_name = 'ecommerce-data-project-bucket'
    object_name = f'{table_name}.csv'
    gcs_hook.upload(bucket_name, object_name, data=csv_data, mime_type='text/csv')

table_names = ['customers', 'geolocation', 'order_items', 'order_payments', 
               'order_reviews', 'orders', 'products', 'sellers']

for table in table_names:
    task = PythonOperator(
        task_id=f'transfer_{table}',
        python_callable=transfer_postgresql_to_gcs,
        op_kwargs={'table_name': table},
        dag=dag,
    )

if __name__ == "__main__":
    dag.cli()