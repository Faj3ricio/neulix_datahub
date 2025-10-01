from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from neulix_dataflow.spiders.linkedin_spider import LinkedInSpider

def run_linkedin_spider():
    spider = LinkedInSpider()
    spider.run()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'linkedin_spider_dag',
    default_args=default_args,
    description='Run LinkedIn spider',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

run_spider_task = PythonOperator(
    task_id='run_linkedin_spider',
    python_callable=run_linkedin_spider,
    dag=dag,
)
