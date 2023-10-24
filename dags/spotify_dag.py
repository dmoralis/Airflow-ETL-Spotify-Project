try:
    import os
    import sys
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sys.path.insert(0, parent_dir)
    from datetime import timedelta, datetime
    from airflow import DAG
    from spotify_methods import fetch_data_and_create_tables, transform_and_load_data
    from airflow.operators.python_operator import PythonOperator
    import pandas as pd
    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


with DAG(
        dag_id="spotify_etl_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2023, 10, 23)
        }) as f:
    run_fetch = PythonOperator(
        task_id="Fetch_and_create_tables",
        provide_context=True,
        python_callable=fetch_data_and_create_tables
    )

    transform_and_save = PythonOperator(
        task_id="transform_and_load_data",
        provide_context=True,
        python_callable=transform_and_load_data
    )

    run_fetch >> transform_and_save
