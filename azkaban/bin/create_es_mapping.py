import os
import sys
import csv
import json


def correct_types(dType):
    dType = dType.lower()
    if dType == 'string':
        dType = 'keyword'
    elif dType == 'date':
        dType = 'date'
    elif dType == 'datetime':
        dType = 'date'
    elif dType == 'integer':
        dType = 'long'
    elif dType == 'long':
        dType = 'long'
    elif dType == 'double':
        dType = 'double'
    else:
        raise(Exception('DataType %s is not supported' % dType))
    return {'type': dType}

def get_nested_features(filename):
    nested_features = []

    file = open(filename, 'r')

    reader = csv.DictReader(file)

    for line in reader:
        if line['Datatype'] == 'Nested':
            nested_features.append(line['Name'])

    return nested_features

def get_embedded_features(filename, nested_features):
    features = {}

    file = open(filename, 'r')
    reader = csv.DictReader(file)

    for line in reader:
        splitName = line['Name'].split('.')
        if len(splitName) > 1 and splitName[0] in nested_features:
            feature = correct_types(line['Datatype'])
            parent = splitName[0]
            if parent in features:
                features[parent]['properties'][splitName[1]] = feature
            else:
                features[parent] = {'type': 'nested', 'properties': {}}
                features[parent]['properties'][splitName[1]] = feature

    return features
        


if __name__ == '__main__':
    feature_list = sys.argv[1]
    mapping_version = sys.argv[2]
    out_path = sys.argv[3]
    in_path = sys.argv[4]

    in_file = open(in_path, 'r')

    mapping_template = json.load(in_file)

    nested_features = get_nested_features(feature_list)

    embedded_features = get_embedded_features(feature_list, nested_features)

    embedded_features['segments'] = {'type': 'long'}

    mapping_template['index_patterns'] = [mapping_version + '_*'] #Add current index to mapping

    mapping_template['mappings']['features']['properties'] = embedded_features

    out_file = open(out_path, 'w')

    json.dump(mapping_template, out_file)