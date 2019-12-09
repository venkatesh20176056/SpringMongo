from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from map_features.spark_job_config import default_spark_config

def raw_features_dag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )

    spark_config = default_spark_config(
        '/opt/spark-2.1.0-native/examples/jars/spark-examples_2.11-2.1.0.jar',
        'org.apache.spark.examples.SparkPi',
        ['10']
    )

    example1=SparkSubmitOperator(
        task_id='example_pi_1',
        dag=dag_subdag,
        **spark_config
    )


    example2=SparkSubmitOperator(
        task_id='example_pi_2',
        dag=dag_subdag,
        **spark_config
    )

    example1.set_downstream(example2)

    return dag_subdag