import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from functions.test_task.process import push_delta

args = {
    'owner': 'airflow',
    'start_date': dt.datetime.today(),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=15),
    'depends_on_past': False,
}

CONFIG = {
    'oper_to_mrr': {
        'source': 'oper',
        'destination': 'mrr'
    },
    'mrr_to_stg': {
        'source': 'mrr',
        'destination': 'stg'
    },
    'stg_to_dwh': {
        'source': 'stg',
        'destination': 'dwh'
    }
}

with DAG(dag_id='test_task',
         default_args=args,
         max_active_tasks=1,
         schedule_interval=None) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    task1 = PythonOperator(
        task_id='oper_to_mrr',
        python_callable=push_delta,
        op_kwargs=CONFIG['oper_to_mrr']
    )
    task2 = PythonOperator(
        task_id='mrr_to_stg',
        python_callable=push_delta,
        op_kwargs=CONFIG['mrr_to_stg']
    )
    task3 = PythonOperator(
        task_id='stg_to_dwh',
        python_callable=push_delta,
        op_kwargs=CONFIG['stg_to_dwh']
    )

    start >> task1 >> task2 >> task3 >> finish
