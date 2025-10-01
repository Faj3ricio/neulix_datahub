from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from neulix_dataflow.spiders.example_spider import WikipediaSpider

def run_wikipedia_spider():
    spider = WikipediaSpider()
    spider.run()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wikipedia_spider_dag',
    default_args=default_args,
    description='Run Wikipedia spider',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

run_spider_task = PythonOperator(
    task_id='run_wikipedia_spider',
    python_callable=run_wikipedia_spider,
    dag=dag,
)
