from datetime import datetime, timedelta
import os
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import IMAPAttachmentOperator
import airflow.hooks.S3_hook
import logging


def upload_file_to_S3(file_name, key, bucket_name):
    '''
    Uploads a local file to s3.
    '''
    hook = airflow.hooks.S3_hook.S3Hook('S3_conn_id')
    hook.load_file(file_name, key, bucket_name)
    logging.info(
        'loaded {} to s3 bucket:{} as {}'.format(file_name, bucket_name, key))


def remove_file(file_name, local_path):
    '''
    Removes a local file.
    '''
    file_path = os.path.join(local_path, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)
        logging.info('removed {}'.format(file_path))


class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')

default_args = {
    'owner': 'harry_daniels',
    'wait_for_downstream': True,
    'start_date': datetime(2019, 8, 3),
    'end_date': datetime(2019, 8, 4),
    'retries': 3,
    'retries_delay': timedelta(minutes=5)}

dag = DAG('imap_example_dag',
          schedule_interval='@daily',
          default_args=default_args)

reporting_date = '{{ yesterday_ds_nodash }}'

start = DummyOperator(
    task_id='start',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)

file_name = 'email_attachment_recieved_on_{}'.format(reporting_date)
local_path = 'staging_folder'
bucket_name = 'email_attachment_bucket'

get_email_attachment = IMAPAttachmentOperator(
    imap_conn_id='imap_conn_id',
    mailbox='mail_test',
    search_criteria={"FROM": "no_reply@example.com",
                     "SUBJECT": "Report"},
    local_path=local_path,
    file_name=file_name,
    task_id='get_email_attachment',
    dag=dag)

upload_attachment_to_s3 = ExtendedPythonOperator(
    python_callable=upload_file_to_S3,
    op_kwargs={'filename': file_name,
               'key': file_name,
               'bucket_name': bucket_name},
    task_id='upload_attachment_to_s3',
    dag=dag)

remove_local_attachment = ExtendedPythonOperator(
    python_callable=remove_file,
    op_kwargs={'file_name': file_name,
               'local_path': local_path},
    task_id='remove_local_attachment',
    dag=dag)

start >> get_email_attachment
get_email_attachment >> upload_attachment_to_s3
upload_attachment_to_s3 >> remove_local_attachment
remove_local_attachment >> end
