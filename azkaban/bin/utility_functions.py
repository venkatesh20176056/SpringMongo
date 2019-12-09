from datadog import initialize, statsd
from requests import HTTPError
import subprocess, sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from email.mime.application import MIMEApplication

def send_email_with_attachment(smtp_server, from_address, to_address, cc_address, subject, body, format='plain', attachments=[]):
    body = body.encode("utf8")

    message = MIMEMultipart()
    message['From'] = from_address
    message['To'] = to_address
    message['Cc'] = cc_address
    message['Subject'] = subject
    message.attach(MIMEText(body, format))

    for (file, filename) in attachments:
        part = MIMEApplication(file, Name=filename)
        part['Content-Disposition'] = 'attachment; filename="%s"' % filename
        message.attach(part)

    try:
        server = smtplib.SMTP(smtp_server)
        server.ehlo()
        server.ehlo()
        server.sendmail(from_address, to_address.split(',')+cc_address.split(','), message.as_string())

        return True
    except Exception as e:
        print(e)
        traceback.print_exc()

def send_email(smtp_server, from_address, to_address, cc_address, subject, body):
    message = MIMEMultipart()
    message['From'] = from_address
    message['To'] = to_address
    message['Cc'] = cc_address
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server)
        server.ehlo()
        server.ehlo()
        server.sendmail(from_address, to_address.split(',')+cc_address.split(','), message.as_string())
        return True
    except Exception as e:
        print(e)
        traceback.print_exc()
        return False

def get_armstrong_email_body(container_info, env, job_name, execid, payload):
    '''
    Get email notification body containing newly provisioned containers
    '''
    email_body = '\nThe following lists have been processed by Midgar. Please activate your list and ' \
                 'bind to UI.\n\n'
    format_string = '%-15s\t\t%-20s\t\t\n'
    email_body += format_string % ('List ID', 'List name')
    email_body += format_string % ('=======', '=========')
    for (_, list_id, name) in container_info:
        email_body += format_string % (list_id, name)
    email_body += '\n\n__________________________\n'
    footer = 'This email was sent at %s UTC by %s-%s-%d %s.\n' \
             % (datetime.utcnow(), env, job_name, execid, payload)
    email_body += footer

    return email_body


#s3_path = Path to reports, it can be parametrized example, "s3://midgar-aws-workspace/stg/mapfeatures/measurements/report/dt=%s/"
#s3_path_param = "2018-05-14" #Argument to be passed to report, could be date, example,
#file_type = "csv" # Format of the file to be read
#local_path_format = "/tmp/PropertyUsageReport_dt_%s.%s" # local location where the file will be written
#
#
def copy_s3_file_to_local(s3_path, s3_path_param, file_type, local_path_format, verbose=True):
    '''Return a local location of local file read from remote s3_path'''

    s3_ls_cmd = "aws s3 --region ap-south-1 ls " + s3_path + s3_path_param + "/"

    print(s3_ls_cmd)

    process_ls = subprocess.Popen(s3_ls_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output_ls, error_ls = process_ls.communicate()
    returncode_ls = process_ls.returncode

    s3_remote_file_name = None

    if returncode_ls != 0:
        print(error_ls)
        sys.exit(returncode_ls)
    else:
        if verbose: print('... s3  ls file successfully')
        s3_remote_file_name = [i.split(' ')[-1] for i in output_ls.split('\n') if ".csv" in i][0]

    locally_copied_file_name = None

    if s3_remote_file_name is not None:
        s3_remote_file_abs_location = s3_path + s3_path_param+ "/" + s3_remote_file_name
        local_file_location = local_path_format%(s3_path_param, file_type)
        s3_cp_cmd = "aws s3 --region ap-south-1 cp %s %s"%(s3_remote_file_abs_location, local_file_location)


        process_cp = subprocess.Popen(s3_cp_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output_cp, error_cp = process_cp.communicate()
        returncode_cp = process_cp.returncode

        if returncode_cp != 0:
            print(error_cp)
            sys.exit(returncode_cp)
        else:
            if verbose: print('... s3  copied  to local file successfully')
            locally_copied_file_name = local_file_location

    return locally_copied_file_name


def read_hdfs_file(hdfs_path, verbose=True):
    '''Return a string read from hdfs_path'''
    prog = 'hadoop fs -cat'
    # fixme
    # prog = "cat"
    command = prog + ' ' + hdfs_path
    if verbose:
        print('Executing', command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    returncode = process.returncode
    if returncode != 0:
        print(error)
        sys.exit(returncode)
    else:
        if verbose: print('... hdfs cat file successfully')
        return output.strip()

def set_datadog_metrics_start (batch_name,process_name,env):
    options = {'api_key': '4be36ee5f1f4da1db65a5bb9cb2411e8','app_key': 'cf108a66f16ba2bc08e1c45a99a6def2a658ba61'}
    initialize(**options)
    job_failed_metrics_name='midgar.'+batch_name+'.'+env+'.job_failed'
    job_success_metrics_name='midgar.'+batch_name+'.'+env+'.job_success'
    statsd.gauge(job_failed_metrics_name,0,tags=['job:'+process_name])
    statsd.gauge(job_success_metrics_name,0,tags=['job:'+process_name])


def set_datadog_metrics_complete(batch_name,process_name,env,exit_code):
    options = {'api_key': '4be36ee5f1f4da1db65a5bb9cb2411e8','app_key': 'cf108a66f16ba2bc08e1c45a99a6def2a658ba61'}
    initialize(**options)
    job_failed_metrics_name='midgar.'+batch_name+'.'+env+'.job_failed'
    job_success_metrics_name='midgar.'+batch_name+'.'+env+'.job_success'
    if exit_code > 0:
        statsd.gauge(job_failed_metrics_name,1,tags=['job:'+process_name])
        statsd.gauge(job_success_metrics_name,0,tags=['job:'+process_name])
    else:
        statsd.gauge(job_failed_metrics_name,0,tags=['job:'+process_name])
        statsd.gauge(job_success_metrics_name,1,tags=['job:'+process_name])


def set_datadog_metrics_retries(batch_name,process_name,env,job_attempt):
    options = {'api_key': '4be36ee5f1f4da1db65a5bb9cb2411e8','app_key': 'cf108a66f16ba2bc08e1c45a99a6def2a658ba61'}
    initialize(**options)
    job_retries_metrics_name='midgar.'+batch_name+'.'+env+'.job_retries'
    statsd.gauge(job_retries_metrics_name,job_attempt,tags=['job:'+process_name])


def set_datadog_metrics_memory_required(batch_name,process_name,env, memRequired):
    options = {'api_key': '4be36ee5f1f4da1db65a5bb9cb2411e8','app_key': 'cf108a66f16ba2bc08e1c45a99a6def2a658ba61'}
    initialize(**options)
    print("datadog memory required: " + str(memRequired))
    job_memory_metrics_name='midgar.'+batch_name+'.'+env+'.memory_increase'
    if memRequired:
        statsd.gauge(job_memory_metrics_name,1,tags=['job:'+process_name])
    else:
        statsd.gauge(job_memory_metrics_name,0,tags=['job:'+process_name])


def handle_http_error(response):
    try:
        response.raise_for_status()
        response
    except HTTPError:
        error = response.json().get('error', '')

        if error:
            reason = '\n' + error.get('reason', '')
            index = '\n' + error.get('index', '')
        else:
            reason = '\nNo JSON response'
            index = ''

        status = response.status_code
        raise HTTPError('%s%s%s' % (status, reason, index))
