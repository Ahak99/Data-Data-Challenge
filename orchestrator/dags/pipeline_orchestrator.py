from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess

# Calculate project root and construct absolute paths
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

# Import project components after path configuration
from src.scraper.data_extraction.data_extraction import DataExtraction
from src.scraper.data_transformation.data_transformation import DataTransformation

def run_unittest(test_module, **kwargs):
    """
    Runs unittest for the specified test module with proper PYTHONPATH
    """
    print(f"Running tests in {test_module}...")
    
    # Create environment with updated PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{project_root}{os.pathsep}{env.get('PYTHONPATH', '')}"
    
    result = subprocess.run(
        ["python", "-m", "unittest", test_module],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env  # Pass the modified environment
    )
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception(f"Tests failed in {test_module}. Aborting workflow.")
    else:
        print(f"All tests passed in {test_module}.")

def run_data_extraction(**kwargs):
    """
    Task to run the data extraction process.
    """
    extractor = DataExtraction()
    extractor.run()

def run_data_transformation(**kwargs):
    """
    Task to run the data transformation process.
    """
    destinations = ["silver", "gold"]
    transformer = DataTransformation(destinations=destinations)
    transformer.run()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
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

# Construct test module paths
test_modules = {
    'utils': 'src.scraper.tests.test_utils',
    'data_extraction': 'src.scraper.tests.test_data_extraction',
    'data_transformation': 'src.scraper.tests.test_data_transformation'
}

# --- TESTING TASKS ---
utils_test_task = PythonOperator(
    task_id='run_utils_test',
    python_callable=run_unittest,
    op_kwargs={'test_module': test_modules['utils']},
    dag=dag,
)

data_extraction_test_task = PythonOperator(
    task_id='run_data_extraction_test',
    python_callable=run_unittest,
    op_kwargs={'test_module': test_modules['data_extraction']},
    dag=dag,
)

extraction_task = PythonOperator(
    task_id='data_extraction',
    python_callable=run_data_extraction,
    dag=dag,
)

data_transformation_test_task = PythonOperator(
    task_id='run_data_transformation_test',
    python_callable=run_unittest,
    op_kwargs={'test_module': test_modules['data_transformation']},
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='data_transformation',
    python_callable=run_data_transformation,
    dag=dag,
)

# --- TASK DEPENDENCIES ---
utils_test_task >> data_extraction_test_task >> extraction_task
extraction_task >> data_transformation_test_task >> transformation_task