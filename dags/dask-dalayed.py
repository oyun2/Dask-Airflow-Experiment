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
#delayed example
@dask.delayed
def inc(x):
    time.sleep(.5)
    return x + 1

@dask.delayed
def dec(x):
    time.sleep(.5)
    return x - 1

@dask.delayed
def add(x, y):
    time.sleep(.5)
    return x + y

def delayed_task(N=10):
    client = Client('tcp://dask_scheduler:8786')
    print("Dask dashboard:", client.dashboard_link) # from coiled dag example
    zs = []
    for i in range(N):
        x = inc(i)
        y = dec(2*i)
        z = add(x, y)
        zs.append(z)
    zs = dask.persist(*zs)
    total = dask.delayed(sum)(zs) #computing with delayed
    #total = sum(zs)#.persist() #computing with futures
    total = total.persist()
    return total

#=========================================================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1, 2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

#=========================================================================================

with DAG(
    default_args=default_args,
    dag_id="dask-dalayed",
    description="dask-dalayed",
    start_date=datetime(2022, 1, 1, 2),
    schedule_interval="@daily",
) as dag:

    t2 = PythonOperator(task_id="delayed_task", python_callable=delayed_task, dag=dag)
    
#=========================================================================================

t2
