import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'automated-style-411721'
stg_dataset_name = 'retails_stg_ai'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='key-controller-v0',
    default_args=default_args,
    description='key controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)


# com_type_predictions_formatted_10
com_type_predictions_formatted_10 = f"""
create or replace table {stg_dataset_name}.com_type_predictions_formatted_10 as 
select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result 
from {stg_dataset_name}.com_type_predictions_raw_10
"""
    
create_com_type_predictions_formatted_10 = BigQueryInsertJobOperator(
    task_id="create_com_type_predictions_formatted_10",
    configuration={
        "query": {
            "query": com_type_predictions_formatted_10,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Update Mrds table with com_types_predictions
update_mrds_com_types_predictions = f"""
update {stg_dataset_name}.Mrds 
set com_types_predictions = (
  select json_value(ml_generate_text_llm_result, '$.com_type') 
  from {ai_dataset_name}.com_type_predictions_formatted_10 
  where Dep_ID = cast(json_value(ml_generate_text_llm_result, '$.Dep_ID') as int64)
)
where 1=1
"""

update_mrds_task = BigQueryInsertJobOperator(
    task_id="update_mrds_com_types_predictions",
    configuration={
        "query": {
            "query": update_mrds_com_types_predictions,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)







# DAG structure
start >> create_com_type_predictions_formatted_10 >> end
start >> update_mrds_task >> end