from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Run weather data scripts in sequence and parallel using Airflow',
    schedule_interval='*/5 * * * *',
    catchup=False
)

run_producer = BashOperator(
    task_id='run_producer_script',
    bash_command='/home/anu/Project_API/airflow_venv/bin/python /home/anu/Project_API/producer.py '
                 '>> /home/anu/Project_API/producer_log.txt 2>&1',
    dag=dag
)

# run_spark_stream = BashOperator(
#     task_id='run_spark_consumer',
#     bash_command="""
# /home/anu/spark/spark-3.5.0-bin-hadoop3/bin/spark-submit \
# --jars /home/anu/libs/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
# /home/anu/libs/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
# /home/anu/libs/kafka-clients-3.5.0.jar,\
# /home/anu/libs/hadoop-aws-3.3.4.jar,\
# /home/anu/libs/hadoop-common-3.3.4.jar,\
# /home/anu/libs/aws-java-sdk-bundle-1.11.901.jar \
# /home/anu/Project_API/spark_consumer.py >> /home/anu/Project_API/spark_log.txt 2>&1
# """,
#     dag=dag
# )

run_spark_stream = BashOperator(
    task_id='run_spark_consumer',
    bash_command="""
/home/anu/spark/spark-3.5.0-bin-hadoop3/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
/home/anu/Project_API/spark_consumer.py >> /home/anu/Project_API/spark_log.txt 2>&1
""",
    dag=dag
)


run_kafka_consumer = BashOperator(
    task_id='run_consumer_script',
    bash_command='/home/anu/Project_API/airflow_venv/bin/python /home/anu/Project_API/consumer.py '
                 '>> /home/anu/Project_API/consumer_log.txt 2>&1',
    dag=dag
)

run_producer >> [run_spark_stream, run_kafka_consumer]
