import datetime
from airflow import DAG
from airflow import models
from airflow.contrib.operators import bigquery_operator
from airflow.utils import trigger_rule

default_dag_arg = {
    'start_date' : yesterday,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : datetime.timedelta(minutes=5),
}

bq_dataset_name = 'airflow_bg_dataset_{{ds_nodash}}'
bq_githib_table_id = bq_dataset_name + '.github_commits'
output_file = 'gs://my_bucket/github_commits.csv'

dag = DAG(
    'fdndb',
    schedule_interval= datetime.timedelta(day = 1),
    default_args= default_dag_args
)
bq_airflow_commits_query = 
bigquery_operator.BaseQueryOperator(
    task_id = 'bq_airflow_commits_query',
    bql = """ select commit, subject, message 
    from [bigquery-public-data:github_repos.commits]
    where repo_name contains 'airflow' 
    """,
    destination_dataset_table = bq_githib_table_id
)

export_commits_to_gcs = 
bigquery_to_gcs.BigQueryCloudStorageOperator(
    task_id = 'export_airflow_commite_gcs',
    source_project_dataset_table = bq_github_table_id,
    destination_cloud_storage_uris = [output_file],
    export_format = 'CSV'
)

bq_airflow_commits_query >> export_commits_to_gcs