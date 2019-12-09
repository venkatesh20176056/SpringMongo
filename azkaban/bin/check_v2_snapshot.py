#!/usr/bin/env python

import sys
import time
import requests
import argparse
import traceback
import subprocess
from datetime import datetime, timedelta
from utility_functions import set_datadog_metrics_start, set_datadog_metrics_complete


def main(ingest_v2_database, ingest_v2_table, oldest_acceptable_time, timeout):

    end_time = time.time() + timeout
    period = 15 # retry every 15 sec

    while time.time() < end_time:
        status = -1
        url = 'http://172.16.22.200:80/jobs/get_latest_snapshot/' + ingest_v2_database
        response = requests.get(url)

        if response.status_code == 200:
            snapshot_name = response.json()['snapshot_name']

            # make sure the latest snapshot's timestamp is in the future of oldest_acceptable_time
            if oldest_acceptable_time < datetime.strptime(snapshot_name, '%Y-%m-%d_%H-%M'):

                target_dir = 's3a://paytmlabs-sea1-midgar-data-dropoff/apps/hive/warehouse/{0}.db/.snapshot/{1}/{2}'.format(
                    ingest_v2_database,
                    snapshot_name,
                    ingest_v2_table
                )

                p = subprocess.Popen(["hdfs", "dfs", "-ls", target_dir])
                status = p.wait()
            else:
                print("Expected snapshot is not yet present... trying again in %d" % period)
        else:
            print('Failed to retrieve the latest snapshot name from URL: {0}'.format(url))

        if status != 0:
            print("`%s` is not yet present... trying again in %d" % (target_dir, period))
            time.sleep(period)
        else:
            print("Existence check passed")
            return

    print traceback.format_exc()
    sys.exit(status)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('processName', help=('name of Azkaban job for monitoring'))
    parser.add_argument('env', help=('environment name (e.g. dev, stg, prod)'))
    parser.add_argument('date', help=('target ingest date ({ingestPath}/{date})'))
    parser.add_argument('ingestV2Database', help=('name of the ingest database (e.g. marketplace'))
    parser.add_argument('ingestV2Table', help=('name of the table (e.g. catalog_product_snapshot_v2)'))
    parser.add_argument('--dateDiff', default='1', help=('optional: numbers of days back'))
    parser.add_argument('--ddBatchPostfix', default='', help=('optional: Datadog batch name postfix'))
    parser.add_argument('--timeout', type=int, default=7200, help=('Time to wait for the file'))
    parser.add_argument('--snapshotHour', type=int, default=20, help=('custom snapshot hour'))
    parser.add_argument('--snapshotMinute', type=int, default=0, help=('custom snapshot minute'))
    args = parser.parse_args()

    process_name = args.processName
    env = args.env
    date_differential = int(args.dateDiff)
    # By default, 8pm UTC of the day before is the oldest acceptable time.
    oldest_acceptable_time = (datetime.strptime(args.date, '%Y-%m-%d') - timedelta(days=date_differential)).replace(
        hour=args.snapshotHour, minute=args.snapshotMinute)

    ingest_v2_database = args.ingestV2Database
    ingest_v2_table = args.ingestV2Table

    timeout = args.timeout

    dd_batch_postfix = args.ddBatchPostfix

    batch_name = 'bannerpersonalization'

    if dd_batch_postfix:
        batch_name += '.' + dd_batch_postfix

    set_datadog_metrics_start(batch_name, process_name, env)

    try:
        main(ingest_v2_database, ingest_v2_table, oldest_acceptable_time, timeout)
        set_datadog_metrics_complete(batch_name, process_name, env, 0)
    except:
        print traceback.format_exc()
        set_datadog_metrics_complete(batch_name, process_name, env, 1)
        sys.exit(1)
