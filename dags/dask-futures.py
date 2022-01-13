import time
from datetime import datetime, timedelta
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from dask import delayed
from dask.distributed import Client

#=========================================================================================
#futures example
def load(x):
    time.sleep(.1)
    return np.arange(100_000) + x

def process(x):
    time.sleep(.1)
    return x + 1

def future_task(N=10):
    client = Client('tcp://dask_scheduler:8786')
    futures = []
    for i in range(N):
        x = client.submit(load, i)
        y = client.submit(process, x)
        futures.append(y)
    result = [future.result() for future in futures]
    return futures, result

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 9, 2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

#=========================================================================================

with DAG(
    default_args=default_args,
    dag_id="future_task",
    description="future_task",
    start_date=datetime(2022, 1, 9, 2),
    schedule_interval="@daily",
) as dag:

    t2 = PythonOperator(task_id="future_task", python_callable=future_task, dag=dag)
#=========================================================================================

t2
