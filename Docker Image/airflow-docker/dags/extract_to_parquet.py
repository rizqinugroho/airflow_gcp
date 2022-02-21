# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
)
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.contrib.operators import bigquery_operator

# STEP 2:Define a start date
#In this case yesterday
yesterday = datetime(2022, 2, 19)


# Spark references
SPARK_CODE = ('gs://us-central1-demo-pipeline-d6404171-bucket/spark_files/transformation.py')
dataproc_job_name = 'spark_job_dataproc'

# STEP 3: Set default arguments for the DAG
default_dag_args = {
    'start_date': yesterday,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
    'extract_to_parquet',
    description='DAG for deployment a Dataproc Cluster, and create parquet file from source',
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args
) as dag:


# STEP 5: Set Operators
# BashOperator
# Every operator has at least a task_id and the other parameters are particular for each one, in this case, is a simple BashOperatator this operator will execute a simple echo “Hello World!”
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

# dataproc_operator
# Create small dataproc cluster
    create_dataproc =  dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc',
        project_id=models.Variable.get('project_id'),
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        num_workers=2,
        zone=models.Variable.get('dataproc_zone'),
        region=models.Variable.get('dataproc_region'),
        master_machine_type='n1-standard-4',
        worker_machine_type='n1-standard-4'
    )

    # Run the PySpark job
    run_spark = dataproc_operator.DataProcPySparkOperator(
        task_id='run_spark',
        main=SPARK_CODE,
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        region=models.Variable.get('dataproc_region'),
        job_name=dataproc_job_name
    )

# BashOperator
# Sleep function to have 1 minute to check the dataproc created
#    sleep_process = BashOperator(
#        task_id='sleep_process',
#        bash_command='sleep 60'
#    )

# load to bigquery
    load_reviews_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load-reviews-to-bigquery",
        bucket="hive-example",
        source_objects=["final-project/output/highest_prices.parquet/part*"],
        destination_project_dataset_table="data-engineering-2-329815:data_engineering_4.highest_prices",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

# dataproc_operator
# Delete Cloud Dataproc cluster.
    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc',
        project_id=models.Variable.get('project_id'),
        region=models.Variable.get('dataproc_region'),
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

# STEP 6: Set DAGs dependencies
# Each task should run after have finished the task before.
    print_date >> create_dataproc  >> run_spark >> load_reviews_to_bigquery >> delete_dataproc
    