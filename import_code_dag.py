from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession

def run_pyspark_code():
    # Configura a sessão Spark
    spark = SparkSession.builder.appName("rodar_codigo").getOrCreate()

    # Carrega o código PySpark do Minio
    minio_bucket = "scripts"
    minio_file_key = "desafio2.py"  # Substitua pelo nome do seu arquivo PySpark

    # Download do código PySpark do Minio
    minio_url = f"minio://172.29.8.228:9000/{minio_bucket}/{minio_file_key}"
    local_file = "/tmp/desafio2.py"
    wget.download(minio_url, local_file)

    # Verifica se o código já rodou as 00:00 do dia correspondente
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    if now.hour > 0:
        return

    # Executa o código PySpark
    os.system(f"spark-submit {local_file}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 11),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'import_and_run_pyspark',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Executar diariamente
)

run_pyspark_task = PythonOperator(
    task_id='run_pyspark_code',
    python_callable=run_pyspark_code,
    dag=dag,
)
