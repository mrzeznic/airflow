import airflow
from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
#import os

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'mrzeznic',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('datafile', default_args=default_args)

# path = os.getcwd()

def task_read13():
    print("hello from task13 read data")
    data_2013 = pd.read_json(
        "http://dummy.restapiexample.com/api/v1/employees")
    #data_2013.to_csv(path + '/data/data_2013.csv')
    data_2013.to_csv('../data/data_2013.csv')

def task_read12():
    print("hello from task12 read data")
    data_2012 = pd.read_json(
        "http://dummy.restapiexample.com/api/v1/employees")
    #data_2012.to_csv(path + '/data/data_2012.csv')
    data_2012.to_csv('../data/data_2012.csv')


def task_merge():
    print("tasks 2012")
    #data_2012 = pd.read_csv(path + '/data/data_2012.csv')
    data_2012 = pd.read_csv('../data/data_2012.csv')
    year2012 = data_2012['id'].value_counts(
            sort=True, ascending=True).to_frame()  # 2012 arrests by id
    print("tasks 2013")
    #data_2013 = pd.read_csv(path + '/data/data_2012.csv')
    data_2013 = pd.read_csv('../data/data_2012.csv')
    year2013 = data_2013['id'].value_counts(
            sort=True, ascending=True).to_frame()  # 2013 arrests by id
    m = pd.concat([year2012, year2013], axis=1)
    m.columns = ['year2012', 'year2013']
    #m.to_csv(path + '/data/mdata.csv')
    m.to_csv('../data/mdata.csv')


t1 = BashOperator(
    task_id='read_json_2012',
    python_callable=task_read12(),
    bash_command='python3 ~/airflow/dags/dag_datatest.py',
    dag=dag)

t2 = BashOperator(
    task_id='read_json_2013',
    python_callable=task_read13(),
    bash_command='python3 ~/airflow/dags/dag_datatest.py',
    dag=dag)

t3 = BashOperator(
    task_id='merge',
    python_callable=task_merge(),
    bash_command='python3 ~/airflow/dags/dag_datatest.py',
    dag=dag)

t3.set_upstream(t1)
t3.set_upstream(t2)
