from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from anaplan_api_wrapper import AnaplanAPI

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_anaplan_sequence(**kwargs):
    """
    Execute Anaplan sequence using AnaplanAPI with parameters from kwargs.
    
    Args:
        kwargs: Dictionary containing workspace_id, model_id, process_wake_up,
                file_path, file_name, process_name, locale_name
    """
    email = os.environ.get('ANAPLAN_EMAIL', 'your.email@example.com')
    password = os.environ.get('ANAPLAN_PASSWORD', 'your_password')
    
    # Extract parameters from kwargs
    workspace_id = kwargs.get('workspace_id')
    model_id = kwargs.get('model_id')
    process_wake_up = kwargs.get('process_wake_up')
    file_path = kwargs.get('file_path')
    file_name = kwargs.get('file_name')
    process_name = kwargs.get('process_name')
    locale_name = kwargs.get('locale_name', 'en_US')
    
    anaplan = AnaplanAPI(email, password)
    anaplan.execute_sequence(
        workspace_id,
        model_id,
        process_wake_up,
        file_path,
        file_name,
        process_name,
        locale_name=locale_name
    )

with DAG(
    'anaplan_sequence_dag',
    default_args=default_args,
    description='DAG to execute multiple Anaplan sequences',
    schedule_interval=timedelta(days=1),  # Runs daily; adjust as needed
    start_date=datetime(2025, 6, 17),
    catchup=False,
    tags=['anaplan'],
) as dag:
    
    # Example task 1
    anaplan_task_1 = PythonOperator(
        task_id='run_anaplan_sequence_1',
        python_callable=run_anaplan_sequence,
        op_kwargs={
            'workspace_id': 'YOUR_WORKSPACE_ID_1',
            'model_id': 'YOUR_MODEL_ID_1',
            'process_wake_up': 'YOUR_WAKE_UP_PROCESS_NAME_1',
            'file_path': 'path/to/your/file1.csv',
            'file_name': 'file1.csv',
            'process_name': 'YOUR_MAIN_PROCESS_NAME_1',
            'locale_name': 'en_US'
        },
        provide_context=True,
    )
    
    # Example task 2 (uncomment and configure to add more tasks)
    """
    anaplan_task_2 = PythonOperator(
        task_id='run_anaplan_sequence_2',
        python_callable=run_anaplan_sequence,
        op_kwargs={
            'workspace_id': 'YOUR_WORKSPACE_ID_2',
            'model_id': 'YOUR_MODEL_ID_2',
            'process_wake_up': 'YOUR_WAKE_UP_PROCESS_NAME_2',
            'file_path': 'path/to/your/file2.csv',
            'file_name': 'file2.csv',
            'process_name': 'YOUR_MAIN_PROCESS_NAME_2',
            'locale_name': 'en_US'
        },
        provide_context=True,
    )
    
    # Define task dependencies (e.g., sequential execution)
    anaplan_task_1 >> anaplan_task_2
    """
