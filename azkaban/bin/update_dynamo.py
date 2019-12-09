import boto3
from datetime import datetime, timedelta


# update stage 2 and overall status

# dynamo connection
def connect_dynamo(aws_region: str, table_name: str):
    _dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    table = _dynamodb.Table(table_name)
    return table


def read_job_from_dynamo(table, date_utc: str, id: str):
    response = table.get_item(
        Key={
            'date_utc': date_utc,
            'id': id
        }
    )
    if 'Item' in response:
        return response['Item']
    else:
        return None

def _push_check(search_result, exec_timestamp):
    if search_result is None: # if none -> search push directly
        return True
    else: # even though not none -> its spark executiontimestamp should be same as the given exec_timestamp
        spark_stage_executiontimestamp = search_result['stages'][1]['request_args']['executionTimestamp']
        if spark_stage_executiontimestamp != exec_timestamp:
            return True # not same, search push
        else:
            return False # matched, skip push



def find_campaign_id(table, segment_id: str, exec_timestamp: str):
    # expected output: either paytm-india_push_campaign_XXX_YYYY-MM-DD or paytm-india_daily_XXX_YYYY-MM-DD
    id_val = segment_id.split("_")[1]  # select numeric part
    id_push_search_template = 'paytm-india_push_campaign_{0}_{1}'
    id_daily_search_template = 'paytm-india_daily_{0}_{1}'
    date_format = '%Y-%m-%d'
    date_from_exec_timestamp = exec_timestamp[:10]  # up to Y-M-D
    exec_date = datetime.strptime(date_from_exec_timestamp, date_format)
    camp_name_date = exec_date + timedelta(days=1)
    camp_name_date_str = datetime.strftime(camp_name_date, date_format)
    id_daily_search_key = id_daily_search_template.format(id_val, camp_name_date_str)
    search_result = read_job_from_dynamo(table, date_from_exec_timestamp, id_daily_search_key)
    push_search_flag = _push_check(search_result, exec_timestamp)
    if push_search_flag:
        id_push_search_key = id_push_search_template.format(id_val, camp_name_date_str)
        search_result = read_job_from_dynamo(table, date_from_exec_timestamp, id_push_search_key)
        if search_result is not None:
            id_full_name = id_push_search_key
        else:
            id_full_name = None
    else:
        id_full_name = id_daily_search_key
    if id_full_name is None:  # error
        print('segment {0} full campaign name is not found in dynamo DB either push or daily'.format(segment_id))
        raise KeyError
    return {'id': id_full_name, 'date_utc': date_from_exec_timestamp}


def update_status_to_dynamo(table, date_utc: str, searched_id: str, stage_number: int = 1,
                            new_status: str = 'SUCCEEDED'):
    print('searched campaing id name: {0}'.format(searched_id))
    current_dict = read_job_from_dynamo(table, date_utc, searched_id)
    if not current_dict is None:
        update_job_status = current_dict
        update_job_status['stages'][stage_number][
            'status'] = new_status  # stage status, stage number=1 -> lookalike spark
        update_job_status['status'] = new_status  # job status
        table.put_item(Item=update_job_status)
        confirm_dict = read_job_from_dynamo(table, date_utc, searched_id)
        new_job_status = confirm_dict['status']
        new_stage_status = confirm_dict['stages'][stage_number]['status']
        print('job status changed to {0}, stage status changed to {1}'.format(new_job_status, new_stage_status))
    else:
        print('segment {0} full campaign name is not found in dynamo DB either push or daily'.format(searched_id))
        raise Exception
