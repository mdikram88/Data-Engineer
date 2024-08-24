from datetime import timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago


dag_args = {
    "owner": "meme",
    "start_date": days_ago(0),
    "email": ["somebody@somwhere.com"],
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG(
    "process_web_logs",
    default_args=dag_args,
    description="Dag for processing weblogs",
    schedule_intervals=timedelta(days=1)
)


extract_data = BashOperator(
    task_id="extract_data",
    bash_command = "cut -d" " -f1 /home/project/accesslog.txt \
    > /home/project/extracted_data.txt",
    dag=dag
)


transform_data = BashOperator(
    task_id="transform_data",
    bash_command = "grep '\b198.46.149.143\b' /home/project/extracted_data.txt > \
    /home/project/transformed_data.txt",
    dag=dag
)


load_data = BashOperator(
    task_id="load_data",
    bash_command="tar -cvf /home/project/weblog.tar \
    /home/project/transformed_data.txt",
    dag=dag
)


extract_data >> transform_data >> load_data