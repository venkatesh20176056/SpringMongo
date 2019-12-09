import sys
import json
import requests
from collections import OrderedDict


def update_es_template(es_endpoint, template_path):
    with open(template_path) as f:
        body = json.loads(f.read(), object_pairs_hook=OrderedDict)
        index_pattern = body['index_patterns'][0].strip().rstrip('_*')
        es_template_api = '{es_endpoint}/_template/{index_pattern}'.format(es_endpoint=es_endpoint,
                                                                           index_pattern=index_pattern)

        response = requests.put(es_template_api, json=body)

        try:
            response.raise_for_status()
        except Exception as e:
            print e
            print response.json()
            raise



if __name__ == '__main__':
    es = sys.argv[1].strip().split(':')  # in the format <host>:<port>:<http|https>
    input_path = sys.argv[2]

    es_host = es[-1] + "://" + es[0]
    update_es_template(es_host, input_path)
