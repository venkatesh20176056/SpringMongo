from datetime import datetime

from paytm.look_alike.assembly import job_queue
import pytz


def drive_job_queue():
    now = datetime.now(pytz.utc)
    job_queue.run_iteration(now)
