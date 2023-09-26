try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))




with DAG(
        dag_id="spotify_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2023, 9, 25),
        }) as f:
    run_extract = PythonOperator(
        task_id="extract_task",
        python_callable=first,
        op_kwargs={"name": "Morales"}
    )
    run_transform = PythonOperator(
    task_id="run_transform",
    python_callable=first,
    op_kwargs={"name": "Morales"}
    )

run_extract >> run_transform
# run_load = PythonOperator(
#    task_id="first",
#    python_callable=first,
#    op_kwargs={"name": "Morales"}
# )
