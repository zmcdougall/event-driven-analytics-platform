from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="smoke_test"
    , start_date=datetime(2026, 1, 1)
    , schedule="@daily"
    , catchup=False
    , tags=["tutorial"]
) as dag:

    hello = BashOperator(
        task_id="hello"
        , bash_command="echo 'airflow is running' && date"
    )
