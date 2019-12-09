#!/usr/bin/env python
import os
import sys
import pymysql.cursors
from datetime import date


def identify_missing_features(connection, fname, tenant):
    
    cursorObject = connection.cursor()

    feature_csv = open(fname).readlines()
    feature_service = []

    try:
        sql = "Select feature_name,status from features where client_id=%s"

        cursorObject.execute(sql,tenant)
        feature_service = dict()

        for row in cursorObject:
            feature_service[row[0]] = row[1]

    except Exception as e:
        print "except called", e
        connection.rollback()
        raise

    additions = []
    enable = []
    disable = []

    for line in feature_csv:
        line = line.strip()
        feature_name = line.split(",")[3]
        feature_status = line.split(",")[9]
        if feature_name not in feature_service and feature_name != 'Name':
            additions.append(line.split(",")[0:7] + list(line.split(",")[9]))
        elif feature_status == '1' and feature_service[feature_name] != 1:
            enable.append(feature_name)
        elif feature_status == '0' and feature_service[feature_name] != 0:
            disable.append(feature_name)

    
    return (additions, enable, disable)

def insert_features(connection, additions, tenant):

    cursorObject = connection.cursor()

    flattened_values = [item for sublist in additions for item in sublist]

    sql = """INSERT INTO features (feature_class, vertical, category, feature_name, feature_label, data_type, feature_desc,last_updated, status, client_id) values 
    """ + ",".join("(%s,%s,%s,%s,%s,%s,%s, NOW(), %s, '{tenant}')".format(tenant=tenant) for _ in additions)

    try:
        rows = cursorObject.execute(sql,flattened_values)
        print("{rows} rows inserted into feature service".format(rows=rows))
    except pymysql.err.DataError as e:
        raise

    connection.commit()

def update_features(connection, enable,disable):

    cursorObject = connection.cursor()


    enable_sql = "UPDATE features SET status = 1 where feature_name in (" + "," .join("%s" for _ in enable) + ")"
    disable_sql = "UPDATE features SET status = 0 where feature_name in (" + "," .join("%s" for _ in disable) + ")"
    

    try:
        if len(enable) > 0:
            rows = cursorObject.execute(enable_sql,enable)
            print("%d rows enabled in feature service" % rows)
        if len(disable) > 0:
            rows = cursorObject.execute(disable_sql,disable)
            print("%d rows disabled in feature service" % rows)
    except pymysql.err.DataError as e:
        raise

    connection.commit()



if __name__ == '__main__':
    fname = sys.argv[1]
    env = sys.argv[2]
    baseDir = sys.argv[3]
    targetDate = sys.argv[4]
    tenant = sys.argv[5]

    assert env in {'prod', 'stg'}
    assert tenant in {'paytm-india', 'paytm-merchant-india','paytm-channel-india', 'paytm-device-india'}
    password = os.environ['FEATURE_SERVICE_DB_{env}'.format(env=env).upper()]

    host = ''

    if env == 'prod':
        host = 'map-feature-service-prod.c3s3qnihlivv.ap-south-1.rds.amazonaws.com'
    else:
        host = 'map-feature-service-stg.c3s3qnihlivv.ap-south-1.rds.amazonaws.com'

    db = ''

    if tenant == 'paytm-merchant-india':
        db = 'feature_service_merchant'
    elif tenant == 'paytm-channel-india':
        db = 'feature_service_channel'
    else:
        db = 'feature_service'

    connection = pymysql.connect(host=host, user="fsroot", password=password, db=db, charset="utf8")

    missing_features = identify_missing_features(connection, fname, tenant)

    if len(missing_features[0]) > 0:
        insert_features(connection, missing_features[0], tenant)
    if len(missing_features[2]) > 0 or len(missing_features[1]) > 0:
        update_features(connection, missing_features[1],missing_features[2])

    connection.close()