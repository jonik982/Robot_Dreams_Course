from airflow import DAG
from airflow.operators.bash  import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from python_scripts.train_model import process_iris_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id='process_iris',
    default_args=default_args,
    description='Process and train Iris dataset with dbt and ML',
    schedule_interval=None,
    catchup=False
)

# 1. DBT transform (adjust path to your dbt project and profile)
dbt_transform = BashOperator(
    task_id='dbt_transform',
    bash_command='cd /opt/airflow/dags/dbt/homework && dbt run --models iris_processed --profiles-dir /opt/airflow/dags/dbt --project-dir /opt/airflow/dags/dbt',
    dag=dag
)


# 2. Train model


train_model = PythonOperator(
    task_id='train_model',
    python_callable=process_iris_data,
    dag=dag
)

# 3. Send email
send_email = EmailOperator(
    task_id='send_email',
    to='maks19982011@gmail.com',
    subject='Iris Model Pipeline Completed',
    html_content='The DAG "process_iris" has successfully completed all steps.',
    dag=dag
)

# DAG dependencies
dbt_transform >> train_model >> send_email
