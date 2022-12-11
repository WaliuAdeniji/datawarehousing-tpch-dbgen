import os
from transform_dataset import format_dataset_and_save_locally
from transform_dataset import generate_facts_and_dimension_table
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "tpch_dbgen_367914_all_data")
DELETE_DOWNLOADED_DATASET = AIRFLOW_HOME + "/*.tbl"
DELETE_TRANSFORMED_DATASET = AIRFLOW_HOME + "/*.parquet"

RAW_FILES = [
    "customer.tbl",
    "supplier.tbl",
    "nation.tbl",
    "region.tbl",
    "part.tbl",
    "partsupp.tbl",
    "orders.tbl",
    "lineitem.tbl",
]

CLEAN_FILES = [
    "df_customer.parquet",
    "df_supplier.parquet",
    "df_nation.parquet",
    "df_region.parquet",
    "df_part.parquet",
    "df_partsupp.parquet",
    "df_orders.parquet",
    "df_lineitem.parquet",
]

STARSCHEMA_FILES = [
    "sales.parquet",
    "customer.parquet",
    "supplier.parquet",
    "part.parquet",
]


def multi_copy_raw_data_to_gcs(**kwargs):
    hook = GCSHook()

    for local_file in RAW_FILES:
        filename = f"{AIRFLOW_HOME}/{local_file}"
        object_name = f"raw/{local_file}"

        hook.upload(bucket_name=BUCKET, object_name=object_name, filename=filename)


def multi_copy_clean_data_to_gcs(**kwargs):
    hook = GCSHook()
    for local_file in CLEAN_FILES:
        filename = f"{AIRFLOW_HOME}/{local_file}"
        object_name = f"clean/{local_file}"

        hook.upload(bucket_name=BUCKET, object_name=object_name, filename=filename)


def multi_copy_clean_data_from_gcs(**kwargs):
    hook = GCSHook()
    for gcs_file in CLEAN_FILES:
        filename = f"{AIRFLOW_HOME}/{gcs_file}"
        object_name = f"clean/{gcs_file}"

        hook.download(bucket_name=BUCKET, object_name=object_name, filename=filename)


def multi_copy_starschema_files_to_gcs(**kwargs):
    hook = GCSHook()
    for local_file in STARSCHEMA_FILES:
        filename = f"{AIRFLOW_HOME}/{local_file}"
        object_name = f"clean/{local_file}"

        hook.upload(bucket_name=BUCKET, object_name=object_name, filename=filename)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="tpch_dbgen_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["tpch_dbgen"],
) as dag:

    with TaskGroup(
        "extract_unzip_upload_rawdata_to_gcs"
    ) as extract_unzip_test_upload_rawdata_to_gcs:

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -o {AIRFLOW_HOME}/customer.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/customer.tbl;\
                           curl -o {AIRFLOW_HOME}/supplier.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/supplier.tbl;\
                           curl -o {AIRFLOW_HOME}/nation.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/nation.tbl;\
                           curl -o {AIRFLOW_HOME}/region.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/region.tbl;\
                           curl -o {AIRFLOW_HOME}/part.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/part.tbl;\
                           curl -o {AIRFLOW_HOME}/partsupp.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/partsupp.tbl;\
                           curl -o {AIRFLOW_HOME}/orders.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/orders.tbl;\
                           curl -o {AIRFLOW_HOME}/lineitem.tbl https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/datasets/lineitem.tbl",
        )

        upload_raw_data_to_gcs_task = PythonOperator(
            task_id="upload_raw_data_to_gcs_task",
            python_callable=multi_copy_raw_data_to_gcs,
        )

        (download_dataset_task >> upload_raw_data_to_gcs_task)

    with TaskGroup(
        "transform_and_upload_cleandata_to_gcs"
    ) as transform_and_upload_cleandata_to_gcs:

        transform_and_save_locally_task = PythonOperator(
            task_id="transform_and_save_locally",
            python_callable=format_dataset_and_save_locally,
        )

        upload_clean_data_to_gcs_task = PythonOperator(
            task_id="upload_clean_data_to_gcs_task",
            python_callable=multi_copy_clean_data_to_gcs,
        )

        remove_downloaded_dataset_task = BashOperator(
            task_id="remove_downloaded_dataset_task",
            bash_command=f"rm {DELETE_DOWNLOADED_DATASET}",
        )

        remove_transformed_dataset_task = BashOperator(
            task_id="remove_transformed_dataset_task",
            bash_command=f"rm {DELETE_TRANSFORMED_DATASET}",
        )

        (
            transform_and_save_locally_task
            >> upload_clean_data_to_gcs_task
            >> remove_downloaded_dataset_task
            >> remove_transformed_dataset_task
        )

    with TaskGroup("create-data-subsets") as create_data_subsets:

        download_clean_data_from_gcs_task = PythonOperator(
            task_id="download_clean_data_from_gcs_task",
            python_callable=multi_copy_clean_data_from_gcs,
        )

        generate_star_schema_model_task = PythonOperator(
            task_id="generate_star_schema_model",
            python_callable=generate_facts_and_dimension_table,
        )

        upload_starschema_files_to_gcs_task = PythonOperator(
            task_id="upload_starschema_files_to_gcs_task",
            python_callable=multi_copy_starschema_files_to_gcs,
        )

        remove_transformed_dataset_task = BashOperator(
            task_id="remove_transformed_dataset_task",
            bash_command=f"rm {DELETE_TRANSFORMED_DATASET}",
        )

        (
            download_clean_data_from_gcs_task
            >> generate_star_schema_model_task
            >> upload_starschema_files_to_gcs_task
            >> remove_transformed_dataset_task
        )
    with TaskGroup("create-external-tables") as create_external_tables:

        external_table_users = BigQueryCreateExternalTableOperator(
            task_id="external_sales_facts_table",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "fact_sales",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/clean/sales.parquet"],
                },
            },
        )

        external_table_users = BigQueryCreateExternalTableOperator(
            task_id="external_customer_dimension_table",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "dim_customer",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/clean/customer.parquet"],
                },
            },
        )

        external_table_users = BigQueryCreateExternalTableOperator(
            task_id="external_supplier_dimension_table",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "dim_supplier",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/clean/supplier.parquet"],
                },
            },
        )

        external_table_users = BigQueryCreateExternalTableOperator(
            task_id="external_part_dimension_table",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "dim_part",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/clean/part.parquet"],
                },
            },
        )

    (
        extract_unzip_test_upload_rawdata_to_gcs
        >> transform_and_upload_cleandata_to_gcs
        >> create_data_subsets
        >> create_external_tables
    )
