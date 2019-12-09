from utility_functions import copy_s3_file_to_local, send_email_with_attachment
import tabulate
import argparse
import sys
import locale
from datetime import datetime, timedelta
import os
import uuid

class global_vars:
    cmd_args = ["date", "date_offset", "env", "email_content_path", "from_address", "to_address", "subject", "cc"]
    cmd_args_help = ["Date of run", "Offset to subtract from date of run", "stg or prod", "Path to the email data",
                     "Email address of from address", "Email address of to address", "Subject line of email", "Emails being CC'd"]

    file_type  = "csv"
    local_path_format = "/tmp/FieldCheckSanityReport_Group_%s"

def runOSCommand(cmd):
    print cmd
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Spark Job failed')


def sendMail(to_emails, cc_emails, subject, csvFileName, csvDate, groupId):

    message = open(csvFileName, "r").read()
    fileName = "/tmp/check_field_sanity_report_mail_{groupId}_{uuid}.html".format(groupId=groupId, uuid=str(uuid.uuid4()))
    print 'Writing content of csvFileName = %s to fileName = %s' % (csvFileName, fileName)

    with open(fileName, 'w') as aFile:
        aFile.write(message)

    sendMailStatus = False
    try:
        # send mail
        toStr = '"{}"'.format(",".join(to_emails))
        ccStr = '"{}"'.format(",".join(cc_emails))
        subjectStr = '"{}"'.format(subject % groupId)

        mailCommandList = ["mailx", "-v",
                           "-a", '"Content-Type: text/html"',
                           "-s", subjectStr,
                           "-c", ccStr,
                           toStr,
                           "<", fileName]
        mailCommand = " ".join(mailCommandList)
        print(mailCommand)
        runOSCommand(mailCommand)

        # delete the mail file
        rmCommand = " ".join(["rm", "-rf", fileName])
        runOSCommand(rmCommand)
        sendMailStatus = True

    except Exception, e:
        print "Exception in sending mail %s" % e
        sendMailStatus = False

    return sendMailStatus


def main():

    parser = argparse.ArgumentParser(description='Sends field check sanity report Emails')
    for arg, arg_help in zip(global_vars.cmd_args, global_vars.cmd_args_help):
        parser.add_argument(arg, type=str, help=arg_help)
    args = parser.parse_args()
    for arg in global_vars.cmd_args:
        setattr(global_vars, arg, getattr(args,arg))

    date_object = datetime.strptime(global_vars.date, "%Y-%m-%d") - timedelta(days=int(global_vars.date_offset))
    target_date_str = datetime.strftime(date_object, "%Y-%m-%d")

    for groupId in range(1, 4):
        groupPath = global_vars.email_content_path % (global_vars.env, groupId)
        local_file_name = copy_s3_file_to_local(groupPath, target_date_str, global_vars.file_type, global_vars.local_path_format % (groupId) + '_dt_%s.%s', verbose=True)
        print 'Successfully copied email content under %s to local file %s' % (groupPath + target_date_str, local_file_name)
        success = sendMail(
            global_vars.to_address.split(","),
            global_vars.cc.split(","),
            global_vars.subject,
            local_file_name,
            target_date_str,
            groupId)

    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)