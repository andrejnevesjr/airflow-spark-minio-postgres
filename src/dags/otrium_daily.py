"""
Otrium - Challenge

Author: André Junior
Maintaner: André Junior
Date: 2022-09-05
Description: Workflow (DAG) to process data provided by Otrium and deliver the proposed challenge.
Steps:
 - Download data from URL
 - Write out to Airflow temp storage | path => /tmp/data/{year}/{month}/{day}
 - Upload data to AWS S3 (minio) | path =>  otrium/raw/{year}/{month}/{day}
 - Execute transformations and actions with Spark cluster
 - Write out to Postgres

 For further information please consider read the file this_challenge.pdf available on docs folder (root).

"""

import os
import requests
from airflow import models
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.macros import ds_format
from datetime import date, datetime, timedelta

###############################################
# Parameters
###############################################

# DAG START DATE
START_DATE = datetime(2018, 8, 20)
# DAG END DATE
END_DATE = None
# Default S3 Bucket
S3_BUCKET = os.environ.get("S3_BUCKET", "otrium")
# S3 Bucket RAW layer
S3_RAW_KEY = os.environ.get("S3_KEY", "raw")
# Variable with path to temporary data storage on Airflow container
STORAGE = os.environ.get("AIRFLOW_DATA_PATH", "/tmp/data")
# S3 CONNECTION ENV VARIABLE
S3_CONN = os.environ.get("S3_CONN", "minio_conn")
# URL to retrieve data
URL = 'https://cocl.us/BD0211EN_Data'
# SOURCE FILENAME
SOURCE_FILE = 'LabData.zip'

###############################################
# Workflow Functions
###############################################


def get_date_part(current_date: date):
    """Format date to use as partition on S3 Bucket

    Args:
        ds : Airflow variable replaced by execution date

    Returns:
        str: Formated date
    """
    return ds_format(current_date, '%Y-%m-%d', '%Y/%m/%d/')


def generate_filename_path(current_date: date, filename: str) -> str:
    """Generate full path to Airflow temp storage including the partition by date

    Args:
        current_date (date): Airflow variable replaced by execution date
        filename (str): filename on Airflow storage

    Returns:
        file_path (str): Full path on Airflow storage
    """
    # Generate partition by date into S3
    date_part = get_date_part(current_date)

    # Create local storage on first DAG execution
    storage_dir = os.path.join(STORAGE, date_part)
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)

    # Get full path to local file
    file_path = os.path.join(storage_dir, filename)

    return file_path


def get_data(**kwargs) -> None:
    """Get data from URL

    Args:
        url (str): Url to retrieve data
        filename (str): Path where data will placed.
    """
    filename = kwargs.get("filename")
    url = kwargs.get("url")

    # Get full path to local file
    file_path = generate_filename_path(kwargs.get('ds'), filename)

    # Check if file already exits
    is_file = os.path.exists(file_path)

    if not is_file:
        resp = requests.get(url, stream=True)
        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=512):
                # filter out keep-alive new chunks
                if chunk:
                    f.write(chunk)
                    f.flush()


def upload_file(**kwargs) -> None:
    """Upload file to S3 Bucket

    Args:
        ds (date): Airflow variable replaced by execution date
        source_filename (str): Original filename placed on Airflow storage
        bucket (str): S3 Bucket that we are going to write data
        s3_conn (str): Airflow variable to connect to S3
    """

    # Airflow S3 Connection variable
    s3_conn = kwargs.get("s3_conn")

    # S3 Bucket
    bucket = kwargs.get("bucket")

    # File within Airflow local storage
    source_filename = (
        generate_filename_path(
            kwargs.get('ds'),
            kwargs.get("source_filename"))
    )

    # Generate partition by date into S3
    date_part = get_date_part(kwargs.get('ds'))

    # Join full key to write to S3 using original filename
    dest_filename = os.path.join(
        S3_RAW_KEY, date_part, kwargs.get("source_filename"))

    # Upload generated file to Minio
    s3 = S3Hook(s3_conn)
    s3.load_file(source_filename,
                 key=dest_filename,
                 bucket_name=bucket)


default_args = {
    "owner": "andrejr",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["andrejnevesjr@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

###############################################
# DAG Definition
###############################################

with models.DAG(
    "otrium_daily",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Download the file from URL
    get_data_from_url = PythonOperator(
        task_id='get_data_from_url',
        python_callable=get_data,
        provide_context=True,
        op_kwargs={
            'url': URL,
            'filename': SOURCE_FILE
        }
    )

    # Upload data to S3 on corresponding date partition
    upload_file_to_s3 = PythonOperator(
        task_id='upload_file_to_s3',
        provide_context=True,
        python_callable=upload_file,
        op_kwargs={
            'source_filename': SOURCE_FILE,
            'bucket': S3_BUCKET,
            's3_conn': S3_CONN
        }
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    start >> get_data_from_url >> upload_file_to_s3 >> end
