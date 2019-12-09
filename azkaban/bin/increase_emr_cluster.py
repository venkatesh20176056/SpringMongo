import os
import sys
import json
import argparse


def os_run_command(cmd):
    print cmd
    sys.stdout.flush()
    rc = os.system(cmd)
    if rc > 0:
        raise Exception('Spark Job failed')


def get_cluster_id(cluster_name, region):
    cmd = "aws emr list-clusters --region {region}".format(region=region)
    clusters = json.loads(os.popen(cmd).read())
    cluster_id = [c for c in clusters['Clusters'] if c['Name'] == cluster_name and c['Status']['State'] == 'WAITING'][0]
    return cluster_id['Id']


def get_task_id(cluster_id, region, task_name):
    cmd = "aws emr describe-cluster --cluster-id {cluster_id} --region {region}".format(cluster_id=cluster_id, region=region)
    tasks_groups = json.loads(os.popen(cmd).read())
    task_id = [t for t in tasks_groups['Cluster']['InstanceGroups'] if t['Name'] == task_name][0]
    return task_id['Id']


def resize(cluster_name, region, task_name, count):
    cluster_id = get_cluster_id(cluster_name, region)
    task_id = get_task_id(cluster_id, region, task_name)
    cmd = "aws emr modify-instance-groups --instance-groups InstanceGroupId={task_id},InstanceCount={count} --region {region}".format(task_id=task_id, count=count, region=region)
    os_run_command(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('cluster', help='name of cluster to resize')
    parser.add_argument('task', help='name of task group to resize')
    parser.add_argument('count', type=int, help='resize count')
    parser.add_argument('env', help='prod|stg')
    parser.add_argument('--region', default='ap-south-1', help='region where cluster is located, e.g. ap-south-1')
    args = parser.parse_args()

    if args.env == 'prod':
        resize(args.cluster, args.region, args.task, args.count)
    else:
        print 'No operation.'
