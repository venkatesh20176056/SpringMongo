from utility_functions import copy_s3_file_to_local, send_email_with_attachment
import tabulate
import argparse
import sys
import locale
from datetime import datetime, timedelta
import os

class global_vars:
    #change later
    cmd_args = ["date", "date_offset", "env", "report_path", "from_address", "to_address", "subject", "cc"]
    cmd_args_help = ["Date of run", "Offset to subtract from date of run", "stg or prod", "Path to the report data", "Email address of from address",
                     "Email address of to address", "Subject line of email", "Emails being CC'd"]

    table_format = [["used_properties",
                     "feat_first_created_date",
                     "feat_last_created_date",
                     "first_usage_date",
                     "latest_usage_date",
                     "curr_date",
                     "deprecated_days",
                     "stale_days",
                     "deprecated_flag",
                     "newness_flag",
                     "stale_flag"]]

    file_type  = "csv"
    local_path_format = "/tmp/PropertyUsageReport_dt_%s.%s"


def html_builder():
    #html_table = tabulate.tabulate(data, headers=global_vars.column_names, tablefmt="html")
    #html_table = html_table.replace('<table>', \
    #                                '<table border=1 style="border-collapse:collapse;">')
    html_preface = "<h3>Property Usage Report</h3><br/>"
    return "<html><body>" + html_preface  + "</body></html>"

def runOSCommand(cmd):
    print cmd
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Spark Job failed')

def sendMailWithAttachment(to_emails, cc_emails, subject, csvFileName, csvDate, EmailMsg):

    sendMailStatus = False
    try:
        # send mail
        toStr = '"{}"'.format(",".join(to_emails))
        ccStr = '"{}"'.format(",".join(cc_emails))
        subjectStr = '"{}"'.format(subject)

        mailCommandList = ["mailx",
                           "-s", subjectStr,
                           ",".join(to_emails)]

        mailCommand = " ".join(mailCommandList)

        attachCommand = """(uuencode %s %s ; echo "%s") | """%(csvFileName, csvFileName, EmailMsg)
        print(attachCommand+mailCommand)
        runOSCommand(attachCommand+mailCommand)

        # delete the mail file
        rmCommand = " ".join(["rm", "-rf", csvFileName])
        runOSCommand(rmCommand)
        sendMailStatus = True

    except Exception, e:
        print "Exception in sending mail"
        print e
        sendMailStatus = False

    return sendMailStatus


def main():

    parser = argparse.ArgumentParser(description='Sends daily transaction emails.')
    for arg, arg_help in zip(global_vars.cmd_args, global_vars.cmd_args_help):
        parser.add_argument(arg, type=str, help=arg_help)
    args = parser.parse_args()
    for arg in global_vars.cmd_args:
        setattr(global_vars, arg, getattr(args,arg))


    date_object = datetime.strptime(global_vars.date, "%Y-%m-%d") - timedelta(days=int(global_vars.date_offset))
    target_date_str = datetime.strftime(date_object, "%Y-%m-%d")
    global_vars.report_path = global_vars.report_path % (global_vars.env)
    global_vars.subject = global_vars.subject + datetime.strftime(date_object, " - %b %d, %Y")

    local_file_name = copy_s3_file_to_local(global_vars.report_path, target_date_str, global_vars.file_type, global_vars.local_path_format, verbose=True)

    all_property_flags = [i.split(",")[-1] for i in open(local_file_name).read().split("\n")]

    not_stale_flags = [i for i in all_property_flags if i=="NotStale"]

    email_msg = "No of Properties Currently Used in Campaigns(i.e., NotStale) : %s"%(str(len(not_stale_flags)))
    success = sendMailWithAttachment(
        global_vars.to_address.split(","),
        global_vars.cc.split(","),
        global_vars.subject,
        local_file_name,
        target_date_str,
        email_msg)

    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)