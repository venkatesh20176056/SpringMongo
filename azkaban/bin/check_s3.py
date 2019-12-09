#!/usr/bin/env python

import sys
import time
import argparse
import traceback
import subprocess
from datetime import datetime, timedelta
from utility_functions import set_datadog_metrics_start, set_datadog_metrics_complete


def main(key_prefix, target_date, file_name, separator, timeout, partitioned):
    target_dir = ""

    if partitioned == 'false':
        target_dir = key_prefix + "/" + file_name
    else:
        target_dir = key_prefix + separator + target_date + "/" + file_name
    end_time = time.time() + timeout
    period = 15 # retry every 15 sec
    
    while time.time() < end_time:
      p = subprocess.Popen(["aws", "s3", "--region", "ap-south-1", "ls", target_dir])
      status = p.wait()

      if status != 0:
        print("`%s` is not yet present... trying again in %d" % (target_dir, period))
        time.sleep(period)
      else:
        print("Existence check passed")
        return

    print traceback.format_exc()
    raise Exception('Dataset not found!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('processName', help=('name of Azkaban job for monitoring'))
    parser.add_argument('env', help=('environment name (e.g. dev, stg, prod)'))
    parser.add_argument('date', help=('target ingest date ({ingestPath}/{date})'))
    parser.add_argument('keyPrefix', help=('keyprefix ({keyprefix}/{date})'))
    parser.add_argument('--dateFormat', default='%Y/%m/%d', help=('optional: specify date format using %Y, %m, %d (e.g. %Y/%m/%d)'))
    parser.add_argument('--fileName', default='_SUCCESS', help=('optional: file to check'))
    parser.add_argument('--dateDiff', default='1', help=('optional: numbers of days back'))
    parser.add_argument('--ddBatchPostfix', default='', help=('optional: Datadog batch name postfix'))
    parser.add_argument('--dateSeparator', default='/', help=('Value to concatenate date with path with. Default "/"'))
    parser.add_argument('--timeout', type=int, default=7200, help=('Time to wait for the file'))
    parser.add_argument('--partitioned', default='true', help=('Whether this dataset is partitioned by date'))
    parser.add_argument('--failOnMissing', default ='true', help=('Whether this job should fail when the dataset is missing'))
    args = parser.parse_args()

    dateSeparator = args.dateSeparator
    # Azkaban strips whitespace
    if dateSeparator == 'None':
      dateSeparator = ''

    process_name = args.processName
    env = args.env
    date_differential = int(args.dateDiff)
    target_date = (datetime.strptime(args.date, '%Y-%m-%d') - timedelta(days=date_differential)).strftime(args.dateFormat)
    key_prefix = args.keyPrefix
    file_name = args.fileName
    timeout = args.timeout

    partitioned = args.partitioned

    dd_batch_postfix = args.ddBatchPostfix

    batch_name = 'mapfeatures'

    if dd_batch_postfix:
        batch_name += '.' + dd_batch_postfix

    set_datadog_metrics_start(batch_name, process_name, env)

    try:
        main(key_prefix, target_date, file_name, dateSeparator, timeout,partitioned)
        set_datadog_metrics_complete(batch_name, process_name, env, 0)
    except:
        print traceback.format_exc()
        set_datadog_metrics_complete(batch_name, process_name, env, 1)
        if args.failOnMissing == 'true':
            sys.exit(1)
