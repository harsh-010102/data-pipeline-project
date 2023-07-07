from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 4,
    'retry_delay': timedelta(minutes=5)

}


def greet(age, ti):  # ti is xcom pass instance
    name = ti.xcom_pull(task_ids='get_name')
    print(f'Hello My name is {name},'
          f'and My age is {age}')


def get_name():
    return 'Jerry'


with DAG(
        default_args=default_args,
        dag_id='pass_parameter_opearator',
        start_date=datetime(2023, 2, 2),
        schedule_interval='@daily',
        catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age': 20}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
    )

    task2 >> task1
