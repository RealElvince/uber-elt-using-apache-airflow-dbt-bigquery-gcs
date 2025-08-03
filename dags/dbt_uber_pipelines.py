from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

profiles_dir =  '/opt/airflow/include/.dbt'
dbt_project_dir = '/opt/airflow/include/dbt/uber_dbt'

with DAG('dbt_uber_dag',
         schedule_interval=None,
         catchup=False,
         default_args=default_args) as dag:
    
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f"cd {dbt_project_dir} && dbt debug --profiles-dir {profiles_dir}"
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {dbt_project_dir} && dbt run --profiles-dir {profiles_dir}",
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {dbt_project_dir} && dbt test --profiles-dir {profiles_dir}",
    )

    dbt_debug >> dbt_run >> dbt_test

    
