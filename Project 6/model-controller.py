import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'automated-style-411721'
raw_dataset_name = 'retails_raw_af'
stg_dataset_name = 'retails_stg_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='p6-model-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)
wait = TimeDeltaSensor(task_id="wait", delta=duration(seconds=5), dag=dag)

# create stg dataset
create_dataset_stg = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_stg',
    project_id=project_id,
    dataset_id=stg_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# Inventory
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
inventory_sql = (
f"""create or replace table {stg_dataset_name}.Inventory(
    Product_ID STRING,
    Product_Name STRING,
    Beginning_Inventory STRING,
    Inventory_Recieved STRING,
    Inventory_Sold STRING,
    Ending_Inventory STRING,
    Predicted_Sales STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select Product_ID, Product_Name, Beginning_Inventory, Inventory_Recieved, Inventory_Sold, Ending_Inventory, Predicted_Sales,
            'kaggle' as data_source, load_time
        from
            (select Product_ID, Product_Name, Beginning_Inventory, Inventory_Recieved, Inventory_Sold, Ending_Inventory, Predicted_Sales, load_time
            from {raw_dataset_name}.inventory)""")

create_inventory_stg = BigQueryInsertJobOperator(
    task_id="create_inventory_stg",
    configuration={
        "query": {
            "query": inventory_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Mineral
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
mineral_sql = (
f"""create or replace table {stg_dataset_name}.Mineral(
    site_name STRING,
    latitude STRING,
    longitude STRING,
    region STRING,
    country STRING,
    state STRING,
    county STRING,
    com_type STRING,
    commod1 STRING,
    commod2 STRING,
    commod3 STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select site_name, latitude, longitude, region, country, state, county, com_type, commod1, commod2, commod3,
            'kaggle' as data_source, load_time
        from
            (select site_name, latitude, longitude, region, country, state, county, com_type, commod1, commod2, commod3, load_time
            from {raw_dataset_name}.mineral)""")

create_mineral_stg = BigQueryInsertJobOperator(
    task_id="create_mineral_stg",
    configuration={
        "query": {
            "query": mineral_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Mrds
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
mrds_sql = (
f"""create or replace table {stg_dataset_name}.Mrds(
    dep_id STRING,
    url STRING,
    mrds_id STRING,
    mas_id STRING,
    site_name STRING,
    latitude STRING,
    longitude STRING,
    region STRING,
    country STRING,
    state STRING,
    county STRING,
    com_type STRING,
    commod1 STRING,
    commod2 STRING,
    commod3 STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select dep_id, url, mrds_id, mas_id, site_name, latitude, longitude, region, country, state, county, com_type, commod1, commod2, commod3,
            'usgs' as data_source, load_time
        from
            (select dep_id, url, mrds_id, mas_id, site_name, latitude, longitude, region, country, state, county, com_type, commod1, commod2, commod3, load_time
            from {raw_dataset_name}.mrds)""")

create_mrds_stg = BigQueryInsertJobOperator(
    task_id="create_mrds_stg",
    configuration={
        "query": {
            "query": mrds_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Parts
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
parts_sql = (
f"""create or replace table {stg_dataset_name}.Parts(
    p_partkey STRING,
    size_description STRING,
    material STRING,
    p_size STRING,
    p_brand STRING,
    p_name STRING,
    p_container STRING,
    p_mfgr STRING,
    p_retailprice STRING,
    p_comment STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select p_partkey, concat(p_type_array[OFFSET(0)], ' ', p_type_array[OFFSET(1)]) as size_description, p_type_array[OFFSET(2)] as material,
            p_size, p_brand, p_name,  p_container, p_mfgr, p_retailprice, p_comment,
            'bird' as data_source, load_time
        from
            (select p_partkey, split(p_type, ' ') as p_type_array, p_size, p_brand, p_name,  p_container, p_mfgr, p_retailprice, p_comment, load_time
            from {raw_dataset_name}.parts)""")


create_parts_stg = BigQueryInsertJobOperator(
    task_id="create_parts_stg",
    configuration={
        "query": {
            "query": parts_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Partsupp
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
partsupp_sql = (
f"""create or replace table {stg_dataset_name}.Partsupp(
    ps_partkey STRING,
    ps_suppkey STRING,
    ps_supplycost STRING,
    ps_availqty STRING,
    ps_comment STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select ps_partkey, ps_suppkey, ps_supplycost, ps_availqty, ps_comment,
            'bird' as data_source, load_time
        from
            (select ps_partkey, ps_suppkey, ps_supplycost, ps_availqty, ps_comment, load_time
            from {raw_dataset_name}.partsupp)""")

create_partsupp_stg = BigQueryInsertJobOperator(
    task_id="create_partsupp_stg",
    configuration={
        "query": {
            "query": partsupp_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Sales
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
sales_sql = (
f"""create or replace table {stg_dataset_name}.Sales(
    Store STRING,
    Dept STRING,
    Date STRING,
    Day STRING,
    Month STRING,
    Year STRING,
    Weekly_Sales STRING,
    IsHoliday STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select Store, Dept, Date as Date, date_array[OFFSET(0)] as Day, date_array[OFFSET(1)] as Month, date_array[OFFSET(2)] as Year,
            Weekly_Sales, IsHoliday,
            'kaggle' as data_source, load_time
        from
            (select Store, Dept, Date, split(Date, '/') as date_array, Weekly_Sales, IsHoliday, load_time
            from {raw_dataset_name}.sales)""")
    
create_sales_stg = BigQueryInsertJobOperator(
    task_id="create_sales_stg",
    configuration={
        "query": {
            "query": sales_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Supplier
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
supplier_sql = (
f"""create or replace table {stg_dataset_name}.Supplier(
    s_suppkey STRING,
    s_nationkey STRING,
    s_comment STRING,
    s_name STRING,
    s_address STRING,
    area_code STRING,
    phone_number STRING,
    s_acctbal STRING,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select s_suppkey, s_nationkey, s_comment, s_name,  s_address, s_phone_array[OFFSET(0)] as area_code, concat(s_phone_array[OFFSET(1)], '-', s_phone_array[OFFSET(2)]) as phone_number, s_acctbal,
            'bird' as data_source, load_time
        from
            (select s_suppkey, s_nationkey, s_comment, s_name,  s_address, split(s_phone, '-') as s_phone_array, s_acctbal, load_time
            from {raw_dataset_name}.supplier)""")

create_supplier_stg = BigQueryInsertJobOperator(
    task_id="create_supplier_stg",
    configuration={
        "query": {
            "query": supplier_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Transactions
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
transactions_sql = (
f"""create or replace table {stg_dataset_name}.Transactions(
    id STRING,
    date STRING,
    Day STRING,
    Month STRING,
    Year STRING,
    store_nbr STRING,
    family STRING,
    sales STRING,
    onpromotion BOOL,
    data_source STRING,
    load_time TIMESTAMP not null)
    as
        select id, date, date_array[OFFSET(2)] as Day, date_array[OFFSET(1)] as Month, date_array[OFFSET(0)] as Year, store_nbr, family, sales,
            case when onpromotion = '0' then false else true end as on_promotion,
            'kaggle' as data_source, load_time
        from
            (select id, date, split(date, '-') as date_array, store_nbr, family, sales, onpromotion, load_time
            from {raw_dataset_name}.transactions)""")

create_transactions_stg = BigQueryInsertJobOperator(
    task_id="create_transactions_stg",
    configuration={
        "query": {
            "query": transactions_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

start >> create_dataset_stg >> create_inventory_stg >> end
start >> create_dataset_stg >> create_mineral_stg >> end
start >> create_dataset_stg >> create_mrds_stg >> end
start >> create_dataset_stg >> create_parts_stg >> end
start >> create_dataset_stg >> create_partsupp_stg >> end
start >> create_dataset_stg >> create_sales_stg >> end
start >> create_dataset_stg >> create_supplier_stg >> end
start >> create_dataset_stg >> create_transactions_stg >> end
