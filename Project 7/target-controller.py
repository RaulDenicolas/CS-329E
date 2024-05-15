import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

project_id = 'automated-style-411721'
stg_dataset_name = 'retails_stg_af'
csp_dataset_name = 'retails_csp_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p7-target-controller',
    default_args=default_args,
    description='target table controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


# create csp dataset
create_dataset_csp = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_csp',
    project_id=project_id,
    dataset_id=csp_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# create Inventory
inventory_sql = (
f"""
create or replace table {csp_dataset_name}.Inventory(
  Product_ID	STRING,
  Product_Name	STRING,
  Beginning_Inventory	STRING,
  Inventory_Recieved	STRING,
  Inventory_Sold	STRING,
  Ending_Inventory	STRING,
  Predicted_Sales	STRING,
  data_source	STRING,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (Product_ID, Beginning_Inventory, Predicted_Sales, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Inventory
""")
create_inventory_csp = BigQueryInsertJobOperator(
    task_id="create_inventory_csp",
    configuration={
        "query": {
            "query": inventory_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Mineral
mrds_sql = (
f"""
create or replace table {csp_dataset_name}.Mineral(
  site_name STRING,
  latitude STRING,
  longitude	STRING,
  region	STRING,
  country	STRING,
  state	STRING,
  county	STRING,
  com_type	STRING,
  commod1	STRING,
  commod2	STRING,
  commod3	STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (site_name, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Mineral
""")
create_mineral_csp = BigQueryInsertJobOperator(
    task_id="create_mineral_csp",
    configuration={
        "query": {
            "query": mrds_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Mrds
mrds_sql = (
f"""
create or replace table {csp_dataset_name}.Mrds(
  Dep_ID STRING,
  url STRING,
  mrds_id STRING,
  mas_id STRING,
  site_name STRING,
  latitude STRING,
  longitude	STRING,
  region	STRING,
  country	STRING,
  state	STRING,
  county	STRING,
  com_type	STRING,
  commod1	STRING,
  commod2	STRING,
  commod3	STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (Dep_ID, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Mrds
""")
create_mrds_csp = BigQueryInsertJobOperator(
    task_id="create_mrds_csp",
    configuration={
        "query": {
            "query": mrds_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Parts
parts_sql = (
f"""
create or replace table {csp_dataset_name}.Parts(
  p_partkey STRING,
  size_description STRING,
  material STRING,
  p_size STRING,
  p_brand STRING,
  p_name STRING,
  p_container	STRING,
  p_mfgr	STRING,
  p_retailprice	STRING,
  p_comment	STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (p_partkey, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Parts
""")
create_parts_csp = BigQueryInsertJobOperator(
    task_id="create_parts_csp",
    configuration={
        "query": {
            "query": parts_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

'''
# create Parts_Purchased
parts_purchased_sql = (
f"""
create or replace table {csp_dataset_name}.Parts_Purchased(
  Transaction_ID STRING,
  p_partkey STRING,
  store_nbr STRING,
  part_category STRING,
  data_source	STRING,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (Transaction_ID, p_partkey, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Parts_Purchased
""")
create_parts_purchased_csp = BigQueryInsertJobOperator(
    task_id="create_parts_purchased_csp",
    configuration={
        "query": {
            "query": parts_purchased_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
'''

# create Partsupp
partsupp_sql = (
f"""
create or replace table {csp_dataset_name}.Partsupp(
  ps_partkey STRING,
  ps_suppkey STRING,
  ps_supplycost STRING,
  ps_availqty STRING,
  ps_comment STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (ps_partkey, ps_suppkey, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Partsupp
""")
create_partsupp_csp = BigQueryInsertJobOperator(
    task_id="create_partsupp_csp",
    configuration={
        "query": {
            "query": partsupp_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Sales
sales_sql = (
f"""
create or replace table {csp_dataset_name}.Sales(
  Store_ID STRING,
  Dept_ID STRING,
  Date STRING,
  Day STRING,
  Month STRING,
  Year STRING,
  Weekly_Sales STRING,
  IsHoliday STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (Store_ID, Dept_ID, Date, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Sales
""")
create_sales_csp = BigQueryInsertJobOperator(
    task_id="create_sales_csp",
    configuration={
        "query": {
            "query": sales_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Supplier
supplier_sql = (
f"""
create or replace table {csp_dataset_name}.Supplier(
  s_suppkey STRING,
  s_nationkey STRING,
  s_comment STRING,
  s_name STRING,
  s_address STRING,
  area_code STRING,
  phone_number STRING,
  s_acctbal STRING,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (s_suppkey, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Supplier
""")
create_supplier_csp = BigQueryInsertJobOperator(
    task_id="create_supplier_csp",
    configuration={
        "query": {
            "query": supplier_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Transactions
transactions_sql = (
f"""
create or replace table {csp_dataset_name}.Transactions(
  id STRING,
  date STRING,
  Day STRING,
  Month STRING,
  Year STRING,
  store_nbr STRING,
  family STRING,
  sales_amount STRING,
  onpromotion BOOL,
  data_source	STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (Transaction_ID, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Transactions
""")
create_transactions_csp = BigQueryInsertJobOperator(
    task_id="create_transactions_csp",
    configuration={
        "query": {
            "query": transactions_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

start >> create_dataset_csp >> create_inventory_csp >> end
start >> create_dataset_csp >> create_mineral_csp >> end
start >> create_dataset_csp >> create_mrds_csp >> end
start >> create_dataset_csp >> create_parts_csp >> end
#start >> create_dataset_csp >> create_parts_purchased_csp >> end
start >> create_dataset_csp >> create_partsupp_csp >> end
start >> create_dataset_csp >> create_sales_csp >> end
start >> create_dataset_csp >> create_supplier_csp >> end
start >> create_dataset_csp >> create_transactions_csp >> end

