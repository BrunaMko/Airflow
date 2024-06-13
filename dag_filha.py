#DAG Filha

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import datetime
import pendulum
import pprint


def print_filha(**kwargs):
    print("Dag filha")
    pprint.pprint(kwargs)
    return "Filha"


with DAG(
    dag_id = "dag_filha",
    schedule = "00 01 * * *",    
    start_date = pendulum.datetime(2024, 6, 12, tz = "America/Sao_Paulo"),
    catchup = True,
) as dag:
    start= EmptyOperator(task_id = "start")
    wait_principal = ExternalTaskSensor(
        task_id = "wait_principal",
        external_dag_id = "dag_principal",
        external_task_id = "python_principal",
        execution_delta = datetime.timedelta(hours= 1),
        poke_interval = 30,
        mode = "reschedule",        
        )
    python_filha = PythonOperator(task_id= "python_filha", python_callable = print_filha)
    end = EmptyOperator(task_id = "end")

start>>wait_principal>>python_filha>>end

