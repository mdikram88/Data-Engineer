from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash import BashOperator


dag_args =  {
    "owner": "Mohd Ikram",
    "start_date": days_ago(0),
    "email": ["game388019@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG(
    dag_id="ETL_toll_data",
    schedule=timedelta(days=1),
    default_args=dag_args,
    description="Apache Aifrlow Final Assignment"
)

unzip_task = BashOperator(
    task_id="unzip_task",
    bash_command='cd /home/project/airflow/dags/finalassignment && tar -xzf tolldata.tgz',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command='cd /home/project/airflow/dags/finalassignment && cut -d "," -f1-4 ./vehicle-data.csv > ./csv_data.csv',
    dag=dag
)


extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command='cd /home/project/airflow/dags/finalassignment && cut -d " " -f5-7 ./tollplaza-data.tsv > ./tsv_data.csv'
)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command='cd /home/project/airflow/dags/finalassignment && cut -c59-67 ./payment-data.txt | tr " " "," > ./fixed_width_data.csv',
    dag=dag
)

consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command='cd /home/project/airflow/dags/finalassignment && paste ./csv_data.csv ./tsv_data.csv ./fixed_width_data.csv > ./extracted_data.csv',
    dag=dag
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command='cd /home/project/airflow/dags/finalassignment && tr [a-z] [A-Z] < ./extracted_data.csv > ./extracted_data.csv',
    dag=dag
)


unzip_task >> extract_data_from_csv >> extract_data_from_tsv >> \
extract_data_from_fixed_width >> consolidate_data >> transform_data







