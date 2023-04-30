import boto3
from datetime import datetime, timedelta
import time
import json
import os
from urllib.parse import urlparse
from pathlib import Path
from boto3.dynamodb.conditions import Key, Attr


def remove_char(a_str, n):
    first_part = a_str[:n]
    last_part = a_str[n + 1:]
    return first_part + last_part


def get_scroll_id(scroll_char):
    scroll_dict = {
        'n': 'basic_nage',
        'g': 'goshin',
        's': 'shime',
        'w': 'basic_weapons',
        'k': 'kdm',
        'o': 'oku',
        'i': 'shinin',
        'v': 'advanced_nage',
        'j': 'aikijutsu_nage',
        'a': 'advanced_weapons',
        'y': 'basic_yawara',
        'd': 'advanced_yawara',
        'x': 'exercises'
    }
    return scroll_dict[scroll_char]


def convert_to_camel_case(string):
    return ''.join(x.capitalize() or '_' for x in string.split('_'))


def get_stub(file_url):
    file_name = urlparse(file_url).path.split('/')[-1]
    print(file_name)
    return Path(file_name).stem


def extract_stub_list(variations):
    stub_list = []
    for variation in variations:
        print(variation)
        stub = get_stub(variation)
        stub_list.append(stub)
    return stub_list


def update_variations(data, file_url):
    new_stub = get_stub(file_url)
    stub_list = extract_stub_list(data['Variations'])
    if new_stub in stub_list:
        print(f'Already exists in list: {new_stub}')
        for i, stub in enumerate(stub_list):
            if stub == new_stub:
                data['Variations'][i] = file_url
    else:
        data['Variations'].append(file_url)
    return data


def lambda_handler(event, context):
    if "Complete" in event['Records'][0]['Sns']['Subject']:
        print("Complete notification detected")
        print(event)
        file_url = json.loads(event['Records'][0]['Sns']['Message'])['hlsUrl']
        print(file_url)
        file_stem = get_stub(file_url)
        print(file_stem)
        if file_stem.startswith('d'):
            print("art: danzan ryu")
            new_stem = remove_char(file_stem, 0)
            print(new_stem)
            scroll = get_scroll_id(new_stem[0])
            print("scroll: ",scroll)
            scroll_camel_case = convert_to_camel_case(scroll)
            print(scroll_camel_case)
            ddb_client = boto3.client('dynamodb')
            response = ddb_client.list_tables(
                ExclusiveStartTableName=scroll_camel_case,
                Limit=1
            )
            item_stem = remove_char(new_stem, 0)
            print(item_stem)
            table_name = response['TableNames'][0]
            print(table_name)
            table_item = item_stem[0]
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(table_name)
            response = table.scan(
                FilterExpression=Attr('Number').eq(table_item))
            data = response['Items'][0]
            print(data)
            if not data['Variations']:
                data['Variations'] = [file_url]
                put_response = table.put_item( Item=data )
                print(put_response)
            else:
                data = update_variations(data, file_url)
                update_response = table.put_item( Item=data )
                print(update_response)
        else:
            raise Exception("Invalid video file name!")
    else:
        print("Not complete")
        return
