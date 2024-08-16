from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'dbt_transform',
    default_args=default_args,
    description='Transform data in BigQuery using dbt',
    schedule_interval='@daily',
)

# Define the path to your dbt project
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_PROFILES_DIR = '/opt/airflow/dbt'

def run_dbt_command(command):
    return f"dbt {command} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"

# Run dbt debug
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=run_dbt_command('debug'),
    dag=dag,
)

# Run dbt deps
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=run_dbt_command('deps'),
    dag=dag,
)

# Run dbt compile
dbt_compile = BashOperator(
    task_id='dbt_compile',
    bash_command=run_dbt_command('compile'),
    dag=dag,
)

# Run staging models
dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command=run_dbt_command('run --models staging'),
    dag=dag,
)

# Run intermediate models
dbt_run_intermediate = BashOperator(
    task_id='dbt_run_intermediate',
    bash_command=run_dbt_command('run --models intermediate'),
    dag=dag,
)

# Run final models
dbt_run_final = BashOperator(
    task_id='dbt_run_final',
    bash_command=run_dbt_command('run --models final'),
    dag=dag,
)

# Run dbt test
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=run_dbt_command('test'),
    dag=dag,
)

# Check if final tables were created
check_fct_sales_by_category = BigQueryCheckOperator(
    task_id='check_fct_sales_by_category',
    sql=f'SELECT COUNT(*) FROM `{Variable.get("bigquery_project")}.{Variable.get("bigquery_dataset")}.fct_sales_by_category`',
    use_legacy_sql=False,
    dag=dag,
)

check_fct_avg_delivery_time = BigQueryCheckOperator(
    task_id='check_fct_avg_delivery_time',
    sql=f'SELECT COUNT(*) FROM `{Variable.get("bigquery_project")}.{Variable.get("bigquery_dataset")}.fct_avg_delivery_time`',
    use_legacy_sql=False,
    dag=dag,
)

check_fct_orders_by_state = BigQueryCheckOperator(
    task_id='check_fct_orders_by_state',
    sql=f'SELECT COUNT(*) FROM `{Variable.get("bigquery_project")}.{Variable.get("bigquery_dataset")}.fct_orders_by_state`',
    use_legacy_sql=False,
    dag=dag,
)

# Define the task dependencies
dbt_debug >> dbt_deps >> dbt_compile >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_final >> dbt_test

dbt_test >> check_fct_sales_by_category
dbt_test >> check_fct_avg_delivery_time
dbt_test >> check_fct_orders_by_state