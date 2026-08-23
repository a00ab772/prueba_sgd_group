from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pyspark_emr_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    step_adder = EmrAddStepsOperator(
        task_id='add_pyspark_step',
        job_flow_id='j-XXXXXXXXXX',
        aws_conn_id='aws_default',
        steps=[]
    )