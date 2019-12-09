from utility_functions import copy_s3_file_to_local, send_email_with_attachment
import tabulate
import argparse
import sys
import locale
from datetime import datetime, timedelta
import os
import uuid


htmlString =("""
<!DOCTYPE html>
<html>
<head>
<style>
#customers {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    border-collapse: collapse;
    width: 100%;
}

#customers td, #customers th {
    border: 1px solid #ddd;
    padding: 8px;
}

#customers tr:nth-child(even){background-color: #f2f2f2;}

#customers tr:hover {background-color: #ddd;}

#customers th {
    padding-top: 12px;
    padding-bottom: 12px;
    text-align: left;
    background-color: #4CAF50;
    color: white;
}


</style>
</head>
<body>
<div>
""",
"""
    </table>
</div>
</body>
</html>
    """)

class global_vars:
    #change later
    cmd_args = ["date", "date_offset", "env", "report_path", "from_address", "to_address", "subject", "cc"]
    cmd_args_help = ["Date of run", "Offset to subtract from date of run", "stg or prod", "Path to the report data", "Email address of from address",
                     "Email address of to address", "Subject line of email", "Emails being CC'd"]

    table_format = [["TestCaseName",
                     "TestOperator",
                     "TestOperands",
                     "PassCounts",
                     "FailCounts",
                     "FailedCustIds",
                     "Vertical",
                     "State"]]

    file_type  = "csv"
    local_path_format = "/tmp/SanityReport_dt_%s.%s"


def getHTMLHeader():
    #html_table = tabulate.tabulate(data, headers=global_vars.column_names, tablefmt="html")
    #html_table = html_table.replace('<table>', \
    #                                '<table border=1 style="border-collapse:collapse;">')
    html_preface = "<h3>Property Usage Report</h3><br/>"
    return "<html><body>" + html_preface  + "</body></html>"


def getHTMLTable(rows, csvTestDate):
    header = rows[0]
    data =  rows[1:]
    def getRow(fail_flag, row):
        if fail_flag == True:
            return "<tr style=\"background-color: #E6B0AA\">\n{}\n</tr>".format("\n".join(row))
        else:
            return "<tr style=\"background-color: #52BE80\">\n{}\n</tr>".format("\n".join(row))

    tableItems = [("Failed" in row, ["<td>{}</td>".format(item) for item in row.split(",")]) for row in data]
    tableRows = "\n".join([getRow(failedFlag, row) for failedFlag, row in tableItems])
    tableHeader =  "\n".join(["<th>{}</th>".format(item) for item in header.split(",")])

    emailHeading ="""
    <h2>{}</h2>
    <p>Please investigate the state of following test cases</p>
    <table id="customers">
    <tr>""".format("Snapshot Sanity Date: %s"%(csvTestDate))

    print tableRows
    print tableHeader
    (startString,endString) = htmlString

    html = startString + emailHeading+ tableHeader + "\n</tr>\n" + tableRows +endString

    return html

def runOSCommand(cmd):
    print cmd
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Spark Job failed')


def sendMail(to_emails, cc_emails, subject, csvFileName, csvDate):

    message = getHTMLTable(open(csvFileName, "r").read().split("\n"), csvDate)
    fileName = "/tmp/sanity_mail_{}.html".format(str(uuid.uuid4()))

    with open(fileName, 'w') as aFile:
        aFile.write(message)

    sendMailStatus = False
    try:
        # send mail
        toStr = '"{}"'.format(",".join(to_emails))
        ccStr = '"{}"'.format(",".join(cc_emails))
        subjectStr = '"{}"'.format(subject)

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
        print "Exception in sending mail"
        sendMailStatus = False

    return sendMailStatus


def main():

    parser = argparse.ArgumentParser(description='Sends sanity report Emails')
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

    success = sendMail(
        global_vars.to_address.split(","),
        global_vars.cc.split(","),
        global_vars.subject,
        local_file_name,
        target_date_str)

    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)