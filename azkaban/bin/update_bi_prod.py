#!/usr/bin/env python
import sys
import os
import s3fs
from datetime import datetime, timedelta
import pandas as pd
import pymysql


def update_bi_prod(canada_campaigns):

    host = 'hero-prod-write-replica.cqith8tf0o3l.us-west-2.rds.amazonaws.com'
    connection = pymysql.connect(host=host, user='odette', password=password, db="bi_prod")
    column_types = {'email_id': 'VARCHAR(254)',
                    'sentTime': 'TIMESTAMP',
                    'openedTime': 'TIMESTAMP',
                    'sentCampaign': 'VARCHAR(254)',
                    'openedCampaign': 'VARCHAR(254)',
                    'dt': 'DATE'}

    sys.stderr.write('started writing to mysql.\n')

    try:
        canada_campaigns.to_sql(con=connection, name='sendgrid_email_campaigns', flavor='mysql', if_exists='replace', index=False, dtype=column_types)
    except Exception as e:
        print e
        connection.close()
    else:
        connection.close()
        sys.stderr.write('completed writing to mysql\n')


def get_campaigns(dt):
    """
    :param dt: target date
    """
    fs = s3fs.S3FileSystem()
    files = fs.ls('midgar-aws-workspace/prod/mapfeatures/canada-campaigns/dt={dt}/'.format(dt=dt))
    file_name = [f for f in files if '_SUCCESS' not in f][0]
    campaigns = pd.read_csv(fs.open(file_name, mode='rb'))
    return campaigns

if __name__ == '__main__':
    target_date = sys.argv[1]
    target_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    password = os.environ['BI_PROD_PASSWORD']

    canada_campaigns = get_campaigns(target_date)
    update_bi_prod(canada_campaigns)


