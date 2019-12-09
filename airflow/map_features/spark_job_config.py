from utils import config

_spark_config = {
    'num_executors': 2,
    'executor_cores': 2,
    'executor_memory': '8g',
    'driver_memory': '8g',
    'conn_id': 'spark_default',
    'jars': '/opt/jars/hadoop-aws-2.7.3.jar,/opt/jars/aws-java-sdk-1.7.4.jar',
    'conf': {
        'spark.network.timeout': '360s',
        'spark.driver.maxResultSize': '0',
        'spark.speculation': 'false',
        'spark.sql.parquet.compression.codec': 'snappy',
    },
}


_spark_config['num_executors'] =  int(config.get('spark', 'nExecutors'))
_spark_config['executor_cores'] = int(config.get('spark', 'executorCores'))
_spark_config['executor_memory'] = config.get('spark', 'executorMem')
_spark_config['driver_memory'] = config.get('spark', 'driverMem')

def default_spark_config(application, java_class, application_args = []) :
    spark_config = dict(_spark_config)
    spark_config['application'] = application
    spark_config['java_class'] = java_class
    spark_config['application_args'] = application_args
    return spark_config

def default_raw_spark_config(java_class, application_args = []) :
    spark_config = dict(_spark_config)
    spark_config['application'] = config.get('spark', 'rawTablePath') + '/mapfeatures.jar'
    spark_config['files'] = config.get('spark', 'rawTablePath') + '/resources/log4j.properties'
    spark_config['conf']['spark.driver.extraJavaOptions'] = '/' + config.get('spark', 'flowEnv') + '_emr.conf'
    spark_config['java_class'] = java_class
    spark_config['application_args'] = application_args
    return spark_config

def default_export_spark_config(java_class, application_args = []) :
    spark_config = dict(_spark_config)
    spark_config['application'] = config.get('spark', 'exportTablePath') + '/mapfeatures.jar'
    spark_config['files'] = config.get('spark', 'exportTablePath') + '/resources/log4j.properties'
    spark_config['conf']['spark.driver.extraJavaOptions'] = '/' + config.get('spark', 'flowEnv') + '_emr.conf'
    spark_config['java_class'] = java_class
    spark_config['application_args'] = application_args
    return spark_config

def default_snapshot_spark_config(java_class, application_args = []) :
    spark_config = dict(_spark_config)
    spark_config['application'] = config.get('spark', 'snapshotTablePath') + '/mapfeatures.jar'
    spark_config['files'] = config.get('spark', 'snapshotTablePath') + '/resources/log4j.properties'
    spark_config['conf']['spark.driver.extraJavaOptions'] = '/' + config.get('spark', 'flowEnv') + '_emr.conf'
    spark_config['java_class'] = java_class
    spark_config['application_args'] = application_args
    return spark_config
