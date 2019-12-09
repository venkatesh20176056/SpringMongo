#!/bin/bash
csv_file=$1
env=$2
host="aa1ktsihobtullc.c3s3qnihlivv.ap-south-1.rds.amazonaws.com"
pass=$FEATURE_SERVICE_DB_STG

if [ "$env" == "prod" ]; then
    host="map-feature-service-prod.c3s3qnihlivv.ap-south-1.rds.amazonaws.com"
    pass=$FEATURE_SERVICE_DB_PROD
fi

echo connect to mysql at $host for env: $env
echo importing features from $csv_file

mysql -u fsroot -h $host -p"$pass" --local-infile feature_service -e "

      LOAD DATA LOCAL INFILE '$csv_file'
      INTO TABLE features
      FIELDS TERMINATED BY ','
      LINES TERMINATED BY '\r\n'
      IGNORE 1 LINES
      (feature_class, vertical, category, feature_name, feature_label, data_type, feature_desc)
      SET last_updated=NOW(), status=true, client_id=\"paytm-india\";
"