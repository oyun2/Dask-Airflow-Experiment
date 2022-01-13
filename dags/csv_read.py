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
def r_csv():
    client = Client('tcp://dask_scheduler:8786')
    ddf = dd.read_csv('data/Rand1.csv')
    pddf = ddf.persist()
    sm = []
    for col in pddf.columns:
        sm.append(pddf[col].sum().persist())
        gsm = sum(sm)
        print(f'gsm result is {gsm}')
        finale = gsm.persist()
    return gsm, finale

#=========================================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 28, 2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

#=========================================================================================
with DAG(
    default_args=default_args,
    dag_id="csv_task",
    description="csv_task",
    start_date=datetime(2021, 12, 28, 2),
    schedule_interval="@daily",
) as dag:

    t2 = PythonOperator(task_id="csv_task", python_callable=r_csv, dag=dag)

#=========================================================================================

t2
