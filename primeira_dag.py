from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="primeira_dag",
    start_date=datetime(2024, 6, 11),
    schedule="@daily",
    doc_md=__doc__,
):
    start = EmptyOperator(task_id="start")
    hello = BashOperator(task_id="hello", bash_command="echo Hello World!")
    end = EmptyOperator(task_id="end")


(start >> hello >> end)
