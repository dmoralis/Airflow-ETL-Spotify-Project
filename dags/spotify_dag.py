try:
    import os
    import sys
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sys.path.insert(0, parent_dir)
    from datetime import timedelta, datetime
    from airflow import DAG
    from spotify_methods import spotify_etl
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
            "start_date": datetime(2023, 3, 10)
        }) as f:
    run_etl = PythonOperator(
        task_id="ETL_Spotify",
        python_callable=spotify_etl
    )

