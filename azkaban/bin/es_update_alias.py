import sys
import json
import requests
from collections import OrderedDict


def update_es_alias(es_endpoint, old_index, new_index, index_count, alias):

    command = ",".join([""" {{ "remove" : {{ "index" : "{old_index}_{i}", "alias" : "{alias}_{i}" }} }},
    {{ "add" : {{ "index" : "{new_index}_{i}", "alias" : "{alias}_{i}" }} }} 
    """.format(old_index=old_index, new_index=new_index, i=i, alias=alias) for i in range(1,index_count + 1)])

    body = """
    {{
        "actions" : [
            {command}
        ]
    }}
    """.format(command=command)


    json_body = json.loads(body, object_pairs_hook=OrderedDict)
    es_alias_api = '{es_endpoint}/_aliases'.format(es_endpoint=es_endpoint)
    status = requests.post(es_alias_api, json=json_body)
    print(status.status_code)


#Only update alias if new alias doesn't exist already
def check_alias(es_endpoint, new_index, index_count):
    request = '{es_endpoint}/{new_index}_*/_alias/*'.format(es_endpoint=es_endpoint,new_index=new_index)
    response = requests.get(request)
    if response.status_code == 200 and len(response.json()) == index_count:
        return True
    return False

if __name__ == '__main__':
    es = sys.argv[1].strip().split(':')  # in the format <host>:<port>:<http|https>
    old_index = sys.argv[2]
    new_index = sys.argv[3]
    index_count = int(sys.argv[4])
    alias = sys.argv[5]
    es_host = es[-1] + "://" + es[0]
    alias_exists = check_alias(es_host, new_index, index_count)
    if not alias_exists:
        update_es_alias(es_host, old_index, new_index, index_count, alias)
    else:
        print "Alias already exists, no update performed."
