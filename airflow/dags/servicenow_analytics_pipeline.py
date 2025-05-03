from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DBT_PROJECT_PATH = "/Users/charanlobo/dbt_servicenow_project"
EXCEL_PATH = "/Users/charanlobo/Desktop/data.xlsx"

def verify_database():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT 1")
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise
    finally:
        conn.close()

def ingest_excel_to_postgres():
    try:
        logger.info(f"Reading Excel file from: {EXCEL_PATH}")
        df = pd.read_excel(EXCEL_PATH, engine='openpyxl')
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        
        df.to_sql(
            'servicenow_tickets_raw',
            engine,
            if_exists='replace',
            index=False,
            schema='public'
        )
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        raise

with DAG(
    'servicenow_analytics_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 0,
    },
) as dag:

    verify_db = PythonOperator(
        task_id='verify_database_connection',
        python_callable=verify_database
    )
    
    load_data = PythonOperator(
        task_id='ingest_excel_to_postgres',
        python_callable=ingest_excel_to_postgres
    )

    # DBT Transformation Tasks (in specific order)
    transform_cleaned = BashOperator(
        task_id='transform_servicenow_tickets_cleaned',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && 
        dbt run --select servicenow_tickets_cleaned
        ''',
        env={
            'OBJC_DISABLE_INITIALIZE_FORK_SAFETY': 'YES',
            'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:~/.local/bin'
        }
    )

    transform_category_wise = BashOperator(
        task_id='transform_category_wise_tickets',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && 
        dbt run --select category_wise_tickets
        ''',
        env={
            'OBJC_DISABLE_INITIALIZE_FORK_SAFETY': 'YES',
            'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:~/.local/bin'
        }
    )

    transform_date_extracted = BashOperator(
        task_id='transform_servicenow_tickets_date_extracted',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && 
        dbt run --select servicenow_tickets_date_extracted
        ''',
        env={
            'OBJC_DISABLE_INITIALIZE_FORK_SAFETY': 'YES',
            'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:~/.local/bin'
        }
    )

    transform_avg_resolution = BashOperator(
        task_id='transform_servicenow_tickets_avg_resolution_time',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && 
        dbt run --select servicenow_tickets_avg_resolution_time
        ''',
        env={
            'OBJC_DISABLE_INITIALIZE_FORK_SAFETY': 'YES',
            'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:~/.local/bin'
        }
    )

    transform_closure_rate = BashOperator(
        task_id='transform_servicenow_tickets_closure_rate',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && 
        dbt run --select servicenow_tickets_closure_rate
        ''',
        env={
            'OBJC_DISABLE_INITIALIZE_FORK_SAFETY': 'YES',
            'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:~/.local/bin'
        }
    )

    transform_monthly_summary = BashOperator(
        task_id='transform_servicenow_monthly_ticket_summary',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && 
        dbt run --select servicenow_monthly_ticket_summary
        ''',
        env={
            'OBJC_DISABLE_INITIALIZE_FORK_SAFETY': 'YES',
            'PATH': os.environ.get('PATH', '') + ':/usr/local/bin:~/.local/bin'
        }
    )

    # Set up dependencies
    (verify_db >> load_data >> transform_cleaned >> transform_category_wise 
     >> transform_date_extracted >> transform_avg_resolution 
     >> transform_closure_rate >> transform_monthly_summary)