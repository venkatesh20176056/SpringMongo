import sys
import MySQLdb

dag_input = sys.argv[1]

query = {'delete from xcom where dag_id = "' + dag_input + '"',
         'delete from task_instance where dag_id = "' + dag_input + '"',
         'delete from sla_miss where dag_id = "' + dag_input + '"',
         'delete from log where dag_id = "' + dag_input + '"',
         'delete from job where dag_id = "' + dag_input + '"',
         'delete from dag_run where dag_id = "' + dag_input + '"',
         'delete from dag where dag_id = "' + dag_input + '"' }

def connect(query):
    db = MySQLdb.connect(host="localhost", user="root", passwd="", db="midgar")
    cur = db.cursor()
    cur.execute(query)
    db.commit()
    db.close()
    return

for value in query:
    print value
    connect(value)