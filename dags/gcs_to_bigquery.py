from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='Transfer data from Google Cloud Storage to BigQuery',
    schedule_interval='@daily',
)

# Define schema for each table
table_schemas = {
    'customers': [
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_unique_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_zip_code_prefix', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'customer_city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_state', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    'geolocation': [
        {'name': 'geolocation_zip_code_prefix', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'geolocation_lat', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'geolocation_lng', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'geolocation_city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'geolocation_state', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    'order_items': [
        {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_item_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'seller_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'shipping_limit_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'freight_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    'order_payments': [
        {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_sequential', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'payment_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_installments', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'payment_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    'order_reviews': [
        {'name': 'review_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'review_comment_title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_comment_message', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_creation_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'review_answer_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ],
    'orders': [
        {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_purchase_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_approved_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_delivered_carrier_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_delivered_customer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_estimated_delivery_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ],
    'products': [
        {'name': 'product_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'product_category_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'product_name_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_description_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_photos_qty', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_weight_g', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_length_cm', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_height_cm', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_width_cm', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    'sellers': [
        {'name': 'seller_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'seller_zip_code_prefix', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'seller_city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'seller_state', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
}

# Create BigQuery dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='ecommerce-data-project-432606.ecommerce',
    project_id='ecommerce-data-project-432606',
    dag=dag,
)

for table, schema in table_schemas.items():
    # Create BigQuery table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f'create_table_{table}',
        dataset_id='ecommerce-data-project-432606.ecommerce',
        table_id=table,
        schema_fields=schema,
        dag=dag,
    )

    # Transfer data from GCS to BigQuery
    transfer_to_bq = GCSToBigQueryOperator(
        task_id=f'transfer_{table}_to_bq',
        bucket='ecommerce-data-project-bucket',
        source_objects=[f'{table}.csv'],
        destination_project_dataset_table=f'ecommerce-data-project-432606.ecommerce.{table}',
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        allow_quoted_newlines=True,
        skip_leading_rows=1,
        field_delimiter=',',
        schema_fields=schema,  # Use the same schema here
        dag=dag,
    )

    create_dataset >> create_table >> transfer_to_bq

if __name__ == "__main__":
    dag.cli()