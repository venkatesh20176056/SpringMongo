#!/usr/bin/env python
import sys
import pymysql.cursors
import os
import s3fs
from datetime import datetime, timedelta


def update_possible_values(possible_values, env, client, db):
    host = ''
    if env == 'prod':
        host = 'map-feature-service-prod.c3s3qnihlivv.ap-south-1.rds.amazonaws.com'
    else:
        host = 'map-feature-service-stg.c3s3qnihlivv.ap-south-1.rds.amazonaws.com'

    connection = pymysql.connect(host=host, user="fsroot", password=password, db=db, charset="utf8")
    cursor = connection.cursor()

    sys.stderr.write('truncating features_possible_values table.\n')
    # Truncate table before adding new list of possible values
    try:
        sql = "DELETE FROM features_possible_values;"
        cursor.execute(sql)
    except Exception as e:
        print e
        connection.rollback()
    else:
        connection.commit()

    sys.stderr.write('starting upload\n')
    for idx, line in enumerate(possible_values):
        line = line.strip()
        feature_line_list = line.split("\t")
        feature_name = feature_line_list[0]
        if len(feature_line_list) < 2:
            continue

        try:
            sql = "SELECT feature_id FROM features WHERE feature_name=%s AND client_id=%s"
            cursor.execute(sql, (feature_name, client))
            feature_id = cursor.fetchone()[0]

            feature_values = [(feature_id, feature_name, fval.decode('utf8')) for fval in feature_line_list[1:]]
            insertsql = "INSERT INTO features_possible_values(feature_id,feature_name,feature_value) VALUES (%s,%s,%s)"
            cursor.executemany(insertsql, feature_values)

        except Exception as e:
            print e
            connection.rollback()
        else:
            connection.commit()

        if idx % 10 == 0:
            sys.stderr.write('Uploaded {idx} possible values\n'.format(idx=idx))

    connection.close()


def get_possible_values(env, client, dt):
    """

    :param client: paytm-india | paytm-canada
    :param env: prod | stg
    :param dt: target date
    :return: [feature_name \t feature_value_1 \t feature_value_2 \t ... feature_value_n]
    """
    path = ''
    if client == 'paytm-india':
        path = 'india'
    elif client == 'paytm-canada':
        path = 'hero'
    elif client == 'paytm-merchant-india':
        path = 'merchant'

    fs = s3fs.S3FileSystem()
    files = fs.ls('midgar-aws-workspace/{env}/mapfeatures/features/{path}/values/dt={dt}/values.txt'.format(env=env, path=path, dt=dt))
    files = [f for f in files if '_SUCCESS' not in f]
    lines = []

    for fname in files:
        for idx, line in enumerate(fs.open(fname)):
            if idx % 10 == 0:
                sys.stderr.write('read {idx} lines\n'.format(idx=idx))
            lines.append(line)

    return lines


if __name__ == '__main__':
    target_date = sys.argv[1]
    env = sys.argv[2]
    client = sys.argv[3]

    target_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    assert env in {'prod', 'stg'}
    assert client in {'paytm-india','paytm', 'paytm-merchant-india'}  # adding possible values for paytm-canada hasn't been implemented yet


    database = ''
    if client == 'paytm-merchant-india':
        database = 'feature_service_merchant'
    else:
        database = 'feature_service'

    password = os.environ['FEATURE_SERVICE_DB_{env}'.format(env=env).upper()]

    possible_values = get_possible_values(env, client, target_date)
    update_possible_values(possible_values, env, client, database)
