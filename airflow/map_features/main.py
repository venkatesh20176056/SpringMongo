from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from map_features.raw.dags import raw_features_dag
from map_features.export.dags import metadata_export_dag, snapshot_export_dag
from map_features.snapshot.dags import snapshot_features_dag

DAG_NAME = 'map-features'

default_args = {
    'owner': 'jlam',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 30),
    'email': ['jerry.lam@paytm.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


def should_run_daily(ds, **kwargs):
    prev_execution_date = kwargs['prev_execution_date'].hours
    execution_date = kwargs['execution_date'].hours

    if execution_date == prev_execution_date :
        return "skip_export"
    else:
        return "snapshot_features"

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='@daily')

raw_features = SubDagOperator(
    task_id='raw_features',
    subdag=raw_features_dag(DAG_NAME, 'raw_features', default_args),
    default_args=default_args,
    dag=dag,
)

snapshot_features = SubDagOperator(
    task_id='snapshot_features',
    subdag=snapshot_features_dag(DAG_NAME, 'snapshot_features', default_args),
    default_args=default_args,
    dag=dag,
)

snapshot_export = SubDagOperator(
    task_id='feature_export',
    subdag=snapshot_export_dag(DAG_NAME, 'feature_export', default_args),
    default_args=default_args,
    dag=dag,
)

metadata_export = SubDagOperator(
    task_id='metadata_export',
    subdag=metadata_export_dag(DAG_NAME, 'metadata_export', default_args),
    default_args=default_args,
    dag=dag,
)

cond = BranchPythonOperator(
    task_id='snapshot_gate',
    provide_context=True,
    python_callable=should_run_daily,
    dag=dag)

skip_export = DummyOperator(task_id='skip_export', dag=dag)
cond.set_upstream(raw_features)
snapshot_features.set_upstream(cond)
skip_export.set_upstream(cond)
snapshot_export.set_upstream(snapshot_features)
metadata_export.set_upstream(snapshot_features)
