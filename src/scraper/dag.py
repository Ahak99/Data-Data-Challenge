from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess

# Ensure your project directory is in the PYTHONPATH.
sys.path.insert(0, os.path.abspath('/path/to/your/project'))

def run_pytest(test_path, **kwargs):
    """
    Runs pytest for the specified test path.
    """
    print(f"Running tests in {test_path}...")
    result = subprocess.run(["pytest", test_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception(f"Tests failed in {test_path}. Aborting workflow.")
    else:
        print(f"All tests passed in {test_path}.")

def run_data_extraction(**kwargs):
    """
    Task to run the data extraction process.
    """
    from src.data_extraction.data_extraction import DataExtraction
    extractor = DataExtraction()
    extractor.run()

def run_data_transformation(**kwargs):
    """
    Task to run the data transformation process.
    """
    from src.data_transformation.data_transformation import DataTransformation
    transformer = DataTransformation()
    transformer.run()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'panerai_workflow_with_tests',
    default_args=default_args,
    description='Orchestrates tests, data extraction, and transformation for Panerai watches',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# --- TESTING TASKS ---

# Task: Run utils_test first
utils_test_task = PythonOperator(
    task_id='run_utils_test',
    python_callable=run_pytest,
    op_kwargs={'test_path': 'src/tests/utils_test.py'},
    provide_context=True,
    dag=dag,
)

# Task: Run data extraction test (subtask of extraction)
data_extraction_test_task = PythonOperator(
    task_id='run_data_extraction_test',
    python_callable=run_pytest,
    op_kwargs={'test_path': 'src/tests/data_extraction_test.py'},
    provide_context=True,
    dag=dag,
)

# Task: Run data extraction
extraction_task = PythonOperator(
    task_id='data_extraction',
    python_callable=run_data_extraction,
    provide_context=True,
    dag=dag,
)

# Task: Run data transformation test (subtask of transformation)
data_transformation_test_task = PythonOperator(
    task_id='run_data_transformation_test',
    python_callable=run_pytest,
    op_kwargs={'test_path': 'src/tests/data_transformation_test.py'},
    provide_context=True,
    dag=dag,
)

# Task: Run data transformation
transformation_task = PythonOperator(
    task_id='data_transformation',
    python_callable=run_data_transformation,
    provide_context=True,
    dag=dag,
)

# --- TASK DEPENDENCIES ---
utils_test_task >> data_extraction_test_task >> extraction_task
extraction_task >> data_transformation_test_task >> transformation_task
