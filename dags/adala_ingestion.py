from airflow import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import psycopg2

default_args={
    "owner":"moudawana",
    "retries":3,
    "retry_delay":timedelta(minutes=5)
    
}
def verify_downloads():
    conn=psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cursor=conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM law_documents")
    count=cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"Total documents in DB: {count}")
    if count==0:
        raise ValueError("No documents found in the database")
def verify_chunks():
    conn=psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cursor=conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM chunks")
    count=cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"Total chunks in DB:{count}")
    if count==0:
        raise ValueError("No chunks fouund -Spark job failed")

with DAG(
    dag_id="adala_ingestion",
    default_args=default_args,
    start_date=datetime(2026,2,22),
    schedule_interval="@weekly",
    catchup=False,
    tags=['ingestion','scraping']

)as dag:
  scrape=BashOperator(
    task_id="scrape_adala",
    bash_command="cd  /opt/airflow/scrapy_project && scrapy crawl adala"
  )
  verify_downloads=PythonOperator(
    task_id="verify_downloads",
    python_callable=verify_downloads ,
  )

  spark_extract=BashOperator(
    task_id="spark_extract",
    bash_command="""
     docker exec mizanai-spark-master-1 \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/spark_jobs/postgresql-42.7.3.jar \
        /opt/spark_jobs/extract_text.py
    """,
  )
  verify_chunks=PythonOperator(
    task_id="verify_chunks",
    python_callable=verify_chunks ,)
  
  scrape>>verify_downloads>>spark_extract>>verify_chunks
