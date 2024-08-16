from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'csv_to_postgresql',
    default_args=default_args,
    description='Ingest CSV files into PostgreSQL',
    schedule_interval='@daily',
)

def ingest_csv_to_postgresql(table_name, **kwargs):
    csv_file = f'/opt/airflow/csv_files/{table_name}.csv'
    df = pd.read_csv(csv_file)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    df.to_sql(table_name, engine, if_exists='replace', index=False)

table_names = ['customers', 'geolocation', 'order_items', 'order_payments', 
               'order_reviews', 'orders', 'products', 'sellers']

for table in table_names:
    task = PythonOperator(
        task_id=f'ingest_{table}',
        python_callable=ingest_csv_to_postgresql,
        op_kwargs={'table_name': table},
        dag=dag,
    )

if __name__ == "__main__":
    dag.cli()