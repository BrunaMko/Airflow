from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import pprint


def meu_codigo(**kwargs):
    print("teste")
    pprint.pprint(kwargs)
    return "Final"


with DAG(
    dag_id="python_operator",
    schedule="* * * * *",
    start_date=pendulum.datetime(2024, 6, 12, 1, 15, tz="America/Sao_Paulo"),
    catchup=True,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    python = PythonOperator(task_id="python", python_callable=meu_codigo)

start >> python >> end
