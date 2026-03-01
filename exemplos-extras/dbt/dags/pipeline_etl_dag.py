from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG de exemplo que orquestra os ETLs locais do projeto
default_args = {
    'owner': 'engenharia_de_dados',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='pipeline_etl_local',
    default_args=default_args,
    description='DAG exemplo: gera dados mock, executa full load e incremental',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 21),
    catchup=False,
    tags=['etl', 'example']
) as dag:

    gerar_dados = BashOperator(
        task_id='gerar_dados',
        bash_command='python /opt/airflow/dags/../..//gerador_dados.py -n 500 --save',
    )

    full_load = BashOperator(
        task_id='full_load',
        bash_command='python /opt/airflow/dags/../..//full_load.py -n 500',
    )

    incremental = BashOperator(
        task_id='incremental_scd',
        bash_command='python /opt/airflow/dags/../..//load_incremental.py -n 100 --tipo ambos',
    )

    # Ordem: gerar dados -> full load -> incremental
    gerar_dados >> full_load >> incremental
