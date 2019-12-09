#!/usr/bin/env python

import os
import sys
import argparse
from datetime import datetime, timedelta
from utility_functions import set_datadog_metrics_start, set_datadog_metrics_complete, set_datadog_metrics_retries, set_datadog_metrics_memory_required


def os_run_command(cmd):
    print cmd
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Spark Job failed')


def run_job(jar_path, class_name, driver_memory, properties_file, num_executors, executor_memory, executor_cores,
            queue, main_arg, additional_cmds, additional_args, sparklens_data_dir):

    cmd = """
    /opt/spark-2.3.1-bin-without-hadoop/bin/spark-submit \
      --conf spark.network.timeout=360s \
      --conf spark.sql.broadcastTimeout=1000 \
      --conf spark.sql.warehouse.dir=s3a://midgar-aws-workspace/spark-warehouse/ \
      --conf spark.driver.maxResultSize=0 \
      --conf spark.ui.showConsoleProgress=false \
      --conf spark.hadoop.mapred.output.committer.class=org.apache.hadoop.mapred.FileOutputCommitter \
      --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
      --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=1024m" \
      --conf "spark.rdd.compress=true" \
      --conf "spark.sql.sources.partitionOverwriteMode=STATIC" \
      --conf "spark.sql.sources.partitionColumnTypeInference.enabled=false" \
      --queue {queue} \
      --class {class_name} \
      --master yarn \
      --deploy-mode cluster \
      --num-executors {num_executors} \
      --driver-memory {driver_memory} \
      --driver-java-options "-XX:MaxPermSize=2048m -Dconfig.resource={properties_file}" \
      --executor-memory {executor_memory} \
      --executor-cores {executor_cores} \
      --conf spark.sparklens.reporting.disabled=true \
      --conf spark.sparklens.data.dir={sparklens_data_dir} \
      --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener \
      --conf spark.shuffle.service.enabled=true \
      {additional_cmds} {jar_path} {main_arg} {additional_args}
      """.format(queue=queue,
                 class_name=class_name,
                 num_executors=num_executors,
                 driver_memory=driver_memory,
                 properties_file=properties_file,
                 executor_memory=executor_memory,
                 executor_cores=executor_cores,
                 jar_path=jar_path,
                 main_arg=main_arg,
                 sparklens_data_dir=sparklens_data_dir,
                 additional_cmds=additional_cmds,
                 additional_args=additional_args)

    os_run_command(cmd)

def bumpMem(mem_in):
    mem = mem_in.lower()
    try:
        if mem.endswith("g"):
            unit = "g"
            num = int(mem[:-1])
        elif mem.endswith("gb"):
            unit = "gb"
            num = int(mem[:-2])
        else:
            raise Exception("invalid memory config: " + mem_in)
        new_mem = num * 2
        return str(new_mem) + unit
    except Exception as e:
        print("memory bump exception: ")
        print(e)
        return mem


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('env', help='environment name (stg | prod)')
    parser.add_argument('date', help='target date')
    parser.add_argument('jar', help='path to fat-jar file')
    parser.add_argument('className', help='class name')
    parser.add_argument('processName', help='job name')
    parser.add_argument('driverMem', help='driver memory (e.g. 2g)')
    parser.add_argument('nExecutors', help='num executors (e.g. 12)')
    parser.add_argument('executorMem', help='executor memory (e.g. 8g)')
    parser.add_argument('executorCores', help='number of cores per executor (max 4)')
    parser.add_argument('jobAttempt', help='job attempt number (0 indexed)', type=int)
    parser.add_argument('retries', help='number of retries', type=int)
    parser.add_argument('--optCmds', default='',
                        help='pass in additional commands here (e.g. "--conf spark.akka.frameSize=256")')
    parser.add_argument('--optArgs', default='', help='pass in optional args here (e.g. "optArg1 optArg2")')

    args = parser.parse_args()

    main_arg = (datetime.strptime(args.date, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")

    properties_file = "/" + args.env + "_emr.conf"
    sparklens_data_dir = "s3://map-features-sparklens-analyzer/" + args.env + "/" + \
                         args.processName + "/"+ (datetime.strptime(args.date, '%Y-%m-%d')).strftime("%Y-%m-%d") +"/"

    batch_name = 'mapfeatures'
    queue = "clm"

    set_datadog_metrics_start(batch_name, args.processName, args.env)
    set_datadog_metrics_retries(batch_name,args.processName,args.env,args.jobAttempt)

    increase_mem = (args.jobAttempt >= args.retries) and args.retries > 0
    print("job attempt: " + str(args.jobAttempt))
    print("max retries: " + str(args.retries))
    print("increasing memory: " + str(increase_mem))
    try:
        executorMem = bumpMem(args.executorMem) if increase_mem else args.executorMem
        driverMem = bumpMem(args.driverMem) if increase_mem else args.driverMem
        print("executorMem: " + executorMem)
        print("driverMem: " + driverMem)
        run_job(args.jar, args.className, driverMem, properties_file, args.nExecutors,
                executorMem, args.executorCores, queue, main_arg, args.optCmds, args.optArgs, sparklens_data_dir)

        set_datadog_metrics_complete(batch_name, args.processName, args.env, 0)
        set_datadog_metrics_memory_required(batch_name, args.processName, args.env, increase_mem)

    except Exception as e:
        print e
        if args.jobAttempt >= args.retries:
            set_datadog_metrics_complete(batch_name, args.processName, args.env, 1)
            print("job failed. max retries attempted.")
        else:
            print("job failed. will retry")
        sys.exit(1)

