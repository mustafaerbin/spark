from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG varsayılan argümanları
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG tanımı
with DAG(
        dag_id='spark_job_scheduler',
        default_args=default_args,
        description='Run Spark Job every 10 minutes',
        schedule_interval='*/10 * * * *',  # Her 10 dakikada bir çalıştırılır
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    # Spark işini çalıştıran görev
    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command=(
            'docker exec spark bash -c '
            '"spark-submit --class com.spark.job.Main '
            '/opt/spark/jobs/sparkJob.jar"'
        ),
    )

    run_spark_job
