#!/usr/bin/env python

import sys
import argparse
import traceback
import subprocess
from datetime import datetime, timedelta
from utility_functions import set_datadog_metrics_start, set_datadog_metrics_complete


def run(cmd):
    print cmd
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        print err
        sys.exit(rc)
    return out.strip('\n')


def main(from_path, mapping_path, target_date, separator):
    toDir = mapping_path + separator + target_date
    latestDir = run("hdfs dfs -ls " + from_path + " | awk '{ print $NF }' | sort -r | head -n 1")
    run("hdfs dfs -mkdir -p " + toDir)
    run("hdfs dfs -cp -f " + latestDir + '/* ' + toDir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('processName', help=('name of Azkaban job for monitoring'))
    parser.add_argument('env', help=('environment name (e.g. dev, stg, prod)'))
    parser.add_argument('date', help=('target date ({outputPath}/{date})'))
    parser.add_argument('fromPath', help=('path to copy from in hdfs ({fromPath})'))
    parser.add_argument('outputPath', help=('path to output in hdfs ({outputPath}/{date})'))
    parser.add_argument('--dateFormat', default='%Y-%m-%d', help=('optional: specify date format using %%Y, %%m, %%d (e.g. %%Y-%%m-%%d)'))
    parser.add_argument('--ddBatchPostfix', default='', help=('optional: Datadog batch name postfix'))
    parser.add_argument('--dateSeparator', default='/', help=('Value to concatenate date with path with. Default "/"'))
    args = parser.parse_args()

    date_separator = args.dateSeparator
    # Azkaban strips whitespace
    if date_separator == 'None':
        date_separator = ''

    process_name = args.processName
    env = args.env
    target_date = (datetime.strptime(args.date, '%Y-%m-%d') - timedelta(days=1)).strftime(args.dateFormat)
    from_path = args.fromPath
    output_path = args.outputPath

    dd_batch_postfix = args.ddBatchPostfix

    batch_name = 'bannerpersonalization'

    if dd_batch_postfix:
        batch_name += '.' + dd_batch_postfix

    set_datadog_metrics_start(batch_name, process_name, env)
    try:
        main(from_path, output_path, target_date, date_separator)
        set_datadog_metrics_complete(batch_name, process_name, env, 0)
    except:
        print traceback.format_exc()
        set_datadog_metrics_complete(batch_name, process_name, env, 1)
        sys.exit(1)
