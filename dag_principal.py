# DAG PRINCIPAL

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import pprint


def print_principal(**kwargs):
    print("Teste")
    pprint.pprint(kwargs)
    return "Principal"


with DAG(
    dag_id="dag_principal",
    schedule="00 00 * * *",
    start_date=pendulum.datetime(2024, 6, 13, tz="America/Sao_Paulo"),
    catchup=True,
) as dag:
    start = EmptyOperator(task_id="start")
    principal = PythonOperator(
        task_id="python_principal", python_callable=print_principal
    )
    end = EmptyOperator(task_id="end")


start >> principal >> end
