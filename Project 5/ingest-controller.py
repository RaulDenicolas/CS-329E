import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

project_id = 'automated-style-411721'
dataset_name = 'retails_raw_af'
bucket_name = 'arcs329e_data'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p5-ingest-controller',
    default_args=default_args,
    description='ingest controller dag',
    schedule_interval='@once',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

ingest_start = DummyOperator(task_id="ingest_start", dag=dag)
ingest_end = DummyOperator(task_id="ingest_end", dag=dag)

# create dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_if_needed',
    project_id=project_id,
    dataset_id=dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# load inventory
inventory_table_name = "inventory"
inventory_file_name = "initial_load/warehouse_management_using_AI.csv"
inventory_delimiter = ","
inventory_skip_leading_rows = 1

inventory_schema_full = [
    {"name": "Product_ID", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Product_Name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Beginning_Inventory", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Inventory_Recieved", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Inventory_Sold", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Ending_Inventory", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Predicted_Sales", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

inventory_schema = [
    {"name": "Product_ID", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Product_Name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Beginning_Inventory", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Inventory_Recieved", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Inventory_Sold", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Ending_Inventory", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Predicted_Sales", "type": "STRING", "mode": "NULLABLE"},
]

inventory = TriggerDagRunOperator(
    task_id="inventory",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": inventory_table_name, "file_name": inventory_file_name, \
           "delimiter": inventory_delimiter, "skip_leading_rows": inventory_skip_leading_rows, \
           "schema_full": inventory_schema_full, "schema": inventory_schema},
    dag=dag)

# load mineral
mineral_table_name = "mineral"
mineral_file_name = "initial_load/mineral_ores_around_the_world.csv"
mineral_delimiter = ","
mineral_skip_leading_rows = 1

mineral_schema_full = [
    {"name": "site_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "latitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "region", "type": "STRING", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "county", "type": "STRING", "mode": "NULLABLE"},
    {"name": "com_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod1", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod2", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod3", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

mineral_schema = [
    {"name": "site_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "latitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "region", "type": "STRING", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "county", "type": "STRING", "mode": "NULLABLE"},
    {"name": "com_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod1", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod2", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod3", "type": "STRING", "mode": "NULLABLE"},
]

mineral = TriggerDagRunOperator(
    task_id="mineral",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": mineral_table_name, "file_name": mineral_file_name, \
           "delimiter": mineral_delimiter, "skip_leading_rows": mineral_skip_leading_rows, \
           "schema_full": mineral_schema_full, "schema": mineral_schema},
    dag=dag)

# load mrds
mrds_table_name = "mrds"
mrds_file_name = "initial_load/mrds.csv"
mrds_delimiter = ","
mrds_skip_leading_rows = 1

mrds_schema_full = [
    {"name": "dep_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "mrds_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "mas_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "site_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "latitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "region", "type": "STRING", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "county", "type": "STRING", "mode": "NULLABLE"},
    {"name": "com_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod1", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod2", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod3", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

mrds_schema = [
    {"name": "dep_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "mrds_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "mas_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "site_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "latitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "STRING", "mode": "NULLABLE"},
    {"name": "region", "type": "STRING", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "county", "type": "STRING", "mode": "NULLABLE"},
    {"name": "com_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod1", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod2", "type": "STRING", "mode": "NULLABLE"},
    {"name": "commod3", "type": "STRING", "mode": "NULLABLE"},
]

mrds = TriggerDagRunOperator(
    task_id="mrds",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": mrds_table_name, "file_name": mrds_file_name, \
           "delimiter": mrds_delimiter, "skip_leading_rows": mrds_skip_leading_rows, \
           "schema_full": mrds_schema_full, "schema": mrds_schema},
    dag=dag)

# load parts
parts_table_name = "parts"
parts_file_name = "initial_load/parts.csv"
parts_delimiter = ","
parts_skip_leading_rows = 1

parts_schema_full = [
    {"name": "p_partkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_size", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_brand", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_container", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_mfgr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_retailprice", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

parts_schema = [
    {"name": "p_partkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_size", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_brand", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_container", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_mfgr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_retailprice", "type": "STRING", "mode": "NULLABLE"},
    {"name": "p_comment", "type": "STRING", "mode": "NULLABLE"},
]

parts = TriggerDagRunOperator(
    task_id="parts",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": parts_table_name, "file_name": parts_file_name, \
           "delimiter": parts_delimiter, "skip_leading_rows": parts_skip_leading_rows, \
           "schema_full": parts_schema_full, "schema": parts_schema},
    dag=dag)

# load partsupp
partsupp_table_name = "partsupp"
partsupp_file_name = "initial_load/partsupp.csv"
partsupp_delimiter = ","
partsupp_skip_leading_rows = 1

partsupp_schema_full = [
    {"name": "ps_partkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_suppkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_supplycost", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_availqty", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

partsupp_schema = [
    {"name": "ps_partkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_suppkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_supplycost", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_availqty", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ps_comment", "type": "STRING", "mode": "NULLABLE"},
]

partsupp = TriggerDagRunOperator(
    task_id="partsupp",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": partsupp_table_name, "file_name": partsupp_file_name, \
           "delimiter": partsupp_delimiter, "skip_leading_rows": partsupp_skip_leading_rows, \
           "schema_full": partsupp_schema_full, "schema": partsupp_schema},
    dag=dag)

# load sales
sales_table_name = "sales"
sales_file_name = "initial_load/sales_data_set.csv"
sales_delimiter = ","
sales_skip_leading_rows = 1

sales_schema_full = [
    {"name": "Store", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Dept", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Weekly_Sales", "type": "STRING", "mode": "NULLABLE"},
    {"name": "IsHoliday", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

sales_schema = [
    {"name": "Store", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Dept", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Weekly_Sales", "type": "STRING", "mode": "NULLABLE"},
    {"name": "IsHoliday", "type": "STRING", "mode": "NULLABLE"},
]

sales = TriggerDagRunOperator(
    task_id="sales",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": sales_table_name, "file_name": sales_file_name, \
           "delimiter": sales_delimiter, "skip_leading_rows": sales_skip_leading_rows, \
           "schema_full": sales_schema_full, "schema": sales_schema},
    dag=dag)

# load supplier
supplier_table_name = "supplier"
supplier_file_name = "initial_load/supplier.csv"
supplier_delimiter = ","
supplier_skip_leading_rows = 1

supplier_schema_full = [
    {"name": "s_suppkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_nationkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_phone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_acctbal", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

supplier_schema = [
    {"name": "s_suppkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_nationkey", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_phone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "s_acctbal", "type": "STRING", "mode": "NULLABLE"},
]

supplier = TriggerDagRunOperator(
    task_id="supplier",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": supplier_table_name, "file_name": supplier_file_name, \
           "delimiter": supplier_delimiter, "skip_leading_rows": supplier_skip_leading_rows, \
           "schema_full": supplier_schema_full, "schema": supplier_schema},
    dag=dag)
 
# load transactions
transactions_table_name = "transactions"
transactions_file_name = "initial_load/train.csv"
transactions_delimiter = ","
transactions_skip_leading_rows = 1

transactions_schema_full = [
    {"name": "id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "store_nbr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "family", "type": "STRING", "mode": "NULLABLE"},
    {"name": "sales", "type": "STRING", "mode": "NULLABLE"},
    {"name": "onpromotion", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

transactions_schema = [
    {"name": "id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "store_nbr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "family", "type": "STRING", "mode": "NULLABLE"},
    {"name": "sales", "type": "STRING", "mode": "NULLABLE"},
    {"name": "onpromotion", "type": "STRING", "mode": "NULLABLE"},
]

transactions = TriggerDagRunOperator(
    task_id="transactions",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": transactions_table_name, "file_name": transactions_file_name, \
           "delimiter": transactions_delimiter, "skip_leading_rows": transactions_skip_leading_rows, \
           "schema_full": transactions_schema_full, "schema": transactions_schema},
    dag=dag)


ingest_start >> create_dataset >> inventory >> ingest_end
ingest_start >> create_dataset >> mineral >> ingest_end
ingest_start >> create_dataset >> mrds >> ingest_end
ingest_start >> create_dataset >> parts >> ingest_end
ingest_start >> create_dataset >> partsupp >> ingest_end
ingest_start >> create_dataset >> sales >> ingest_end
ingest_start >> create_dataset >> supplier >> ingest_end
ingest_start >> create_dataset >> transactions >> ingest_end
