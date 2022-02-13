from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import date, timedelta
import datetime


default_args = {
    'owner': 'DH',
    'start_date': datetime.datetime(2022, 1, 28),
    'email': 'danish.hudani78@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment'

)

unzip_data = BashOperator(
    task_id='unzip_data', 
    bash_command='tar -xvzf /home/danish/airflow/dags/tolldata.tgz',
    dag=dag
)


extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/danish/airflow/dags/vehicle-data.csv > csv_data.csv',
    dag=dag
)

commands = """
    cut -f5,6,7 /home/danish/airflow/dags/tollplaza-data.tsv > inter_tsv_data.txt;
    tr "\t" "," < inter_tsv_data.txt > inter_tsv_data02.txt;
    tr -d "\r" < inter_tsv_data02.txt > tsv_data.csv;
"""

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=commands,
    dag=dag
)

commands2 = """
    cut -c59- /home/danish/airflow/dags/payment-data.txt > inter_fixed_width_data.txt;
    tr " " "," < inter_fixed_width_data.txt > inter2.txt;
    tr -d "\r" < inter2.txt > fixed_width_data.csv;

"""

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=commands2,
    dag=dag
)

command4= """
	paste -d "," /home/danish/airflow/dags/csv_data.csv /home/danish/airflow/dags/tsv_data.csv /home/danish/airflow/dags/fixed_width_data.csv > extracted_data.csv
"""


consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=command4,
    dag=dag

)

command3 = """
	awk -F, -v OFS=, '{$4 = toupper($4); print}' /home/danish/airflow/dags/extracted_data.csv > transformed_data.csv;

"""

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=command3,
    dag=dag

)

#task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
