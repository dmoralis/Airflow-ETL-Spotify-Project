try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    from spotify_methods import spotify_etl
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
            "start_date": datetime(2023, 9, 25),
        }) as f:
    run_etl = PythonOperator(
        task_id="ETL_Spotify",
        python_callable=spotify_etl(),
        op_kwargs={"name": "Morales"}
    )

