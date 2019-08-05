
# IMAP Email attachment to S3 Dag

The purpose of this dag is to get a specifc email attachment and upload it S3 each day.

## Requirements

The following airflow plugins are required:
* IMAP_plugin

## Additional Info:
The DAG uses an Extended Python Operator, which inherits from the PythonOperator, and defines the op_kwargs field as a template field. Template fields define which fields get jinjaified.

## Authors:
Harry Daniels: harry.daniels@annalect.com


