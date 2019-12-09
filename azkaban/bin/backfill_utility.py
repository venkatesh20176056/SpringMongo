from azkaban.remote import Session as AzkabanSession
import datetime
import time
import pandas as pd
from dateutil import parser
from datetime import timedelta
import argparse
import sys

AZKABAN_API = "http://azkaban:rmtDOjNkNZPAOGLkybWm123@172.21.9.144:8099"
AZKABAN_PROJECT = "Map-Features-Raw" # Project Name here as shown on azkaban UI
AZKABAN_FLOW = "BaseTableFlow" # Typically the whole flow is executed, if you want a single job you can spcificy it in jobs param of run_workflow method below


def submit_job(date, jobs, base_dir):
    azkaban_session = AzkabanSession(AZKABAN_API)
    # job overriding params can be given here, otherwise job runs with default configurations
    job_properties = {
        'job.targetDate': date,
        'baseDir': base_dir
    }
    workflow = azkaban_session.run_workflow(AZKABAN_PROJECT,
                                            AZKABAN_FLOW,
                                            jobs=jobs,
                                            concurrent='pipeline:2',  # block job A until the previous flow job A's children finish
                                            properties=job_properties,
                                            on_failure='finish',
                                            notify_early=True
                                            )
    execid = workflow['execid']
    print("Submitted job for date {}. Running with id {}".format(date, execid))

    return execid


def kill_job(id):
    azkaban_session = AzkabanSession(AZKABAN_API)
    resp = azkaban_session.cancel_execution(id)
    print(resp)


def get_running_status(id):
    azkaban_session = AzkabanSession(AZKABAN_API)
    status_code = azkaban_session.get_execution_status(id)["status"]
    return status_code


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    arg_parser.add_argument('target_date')
    arg_parser.add_argument('backfill_date', help='Date to backfill to')
    arg_parser.add_argument('lookback_period', help='Number of days that are generated per run. This should be present in the job\'s  .job file')
    arg_parser.add_argument('jobs', help='Comma separated list of jobs to run')
    arg_parser.add_argument('--exitOnFailue', default=True, help='Whether the script should stop on job fialure or continue')
    arg_parser.add_argument('--base_dir', default='/opt/map-features', help='Optional: directory where mapfeatures.jar is, defaults to /opt/map-features')


    args = arg_parser.parse_args()

    jobs_list = args.jobs.split(',')
    base_dir = args.base_dir

    lookback_period = int(args.lookback_period)

    start_date = parser.parse(args.target_date)
    end_date = parser.parse(args.backfill_date)

    required_dates = sorted(pd.date_range(end_date, start_date)[::lookback_period], reverse=True)


    submit_date = start_date

    #looping until end_date is found

    for submit_date in required_dates:
        submit_date_str = submit_date.isoformat().split('T')[0]
        print(submit_date_str)

        #Submit job
        exec_id = submit_job(submit_date_str, jobs_list, base_dir)
        _status = get_running_status(exec_id)

        _log_str1 = "Submitted Job with parameters: %s, Status: %s"%(submit_date_str, _status)

        print(_log_str1)
        sys.stdout.flush()
        time.sleep(30)

        #check for success or preparing
        while ((_status == "RUNNING") or (_status == "PREPARING")):
            sys.stdout.flush()
            time.sleep(30)
            _status = get_running_status(exec_id)
            _current_time = datetime.datetime.now().isoformat()
            _log_str2 = "%s: Checking Job Status: %s"%(_current_time, _status)
            print(_log_str2)

            # do intermittent checks for Running status
            if (_status != "RUNNING"):
                _log_str3 = "%s: Exiting, JobStatus: %s"%(_current_time, _status)
                print(_log_str3)
                if (_status == "FAILED" or _status == "KILLED") and args.exitOnFailue:
                    print('Job Failed, stopping backfill')
                    sys.exit(1)
                else:
                    break
            else:
                continue
