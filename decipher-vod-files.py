import boto3
from datetime import datetime, timedelta
import time
import json
import os
import re
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
        print(f'Updating existing entry: {new_stub}')
        for i, stub in enumerate(stub_list):
            if stub == new_stub:
                data['Variations'][i] = file_url
    else:
        data['Variations'].append(file_url)
    return data


def handle_simple_table_model(stem, table):
    item_stem = remove_char(stem, 0)
    print(item_stem)
    temp = re.findall(r'\d+', item_stem)
    table_item_number = temp[0]
    print(table_item_number)
    response = table.scan(FilterExpression=Attr('Number').eq(table_item_number))
    data = response['Items'][0]
    print(data)
    return data


def handle_basic_yawara(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_advanced_yawara(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_exercises(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_basic_weapons(file_stem, ddb_table):
    pass

def handle_advanced_weapons(file_stem, ddb_table):
    pass

def handle_kdm(file_stem, ddb_table):
    pass

def handle_oku(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_shime(file_stem, ddb_table):
    pass

def handle_shinin(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_goshin(file_stem, ddb_table):
    pass

def handle_basic_nage(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_advanced_nage(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)

def handle_aikijutsu_nage(file_stem, ddb_table):
    return handle_simple_table_model(file_stem, ddb_table)



def handle_scroll(scroll, file_stem, ddb_table):
    data = None
    if scroll == 'basic_yawara':
        print("basic yawara")
        data = handle_basic_yawara(file_stem, ddb_table)
    elif scroll == 'advanced_yawara':
        print("advanced yawara")
        data = handle_advanced_yawara(file_stem, ddb_table)
    elif scroll == 'exercises':
        print("exercises")
        data = handle_exercises(file_stem, ddb_table)
    elif scroll == 'basic_weapons':
        print("basic weapons")
        data = handle_basic_weapons(file_stem, ddb_table)
    elif scroll == 'advanced_weapons':
        print("advanced weapons")
        data = handle_advanced_weapons(file_stem, ddb_table)
    elif scroll == 'kdm':
        print("kdm")
        data = handle_kdm(file_stem, ddb_table)
    elif scroll == 'oku':
        print("oku")
        data = handle_oku(file_stem, ddb_table)
    elif scroll == 'shime':
        print("shime")
        data = handle_shime(file_stem, ddb_table)
    elif scroll == 'basic_nage':
        print("basic nage")
        data = handle_basic_nage(file_stem, ddb_table)
    elif scroll == 'advanced_nage':
        print("advanced nage")
        data = handle_advanced_nage(file_stem, ddb_table)
    elif scroll == 'aikijutsu_nage':
        print("aikijutsu nage")
        data = handle_aikijutsu_nage(file_stem, ddb_table)
    elif scroll == 'shinin':
        print("shinin")
        data = handle_shinin(file_stem, ddb_table)
    elif scroll == 'goshin':
        print("goshin")
        data = handle_goshin(file_stem, ddb_table)
    else:
        print("Unknown scroll: "+scroll)
        raise Exception("Unknown scroll: "+scroll)

    return data


def get_db_table_name_for_scroll(scroll_name):
    scroll_camel_case = convert_to_camel_case(scroll_name)
    print(scroll_camel_case)
    ddb_client = boto3.client('dynamodb')
    response = ddb_client.list_tables(
        ExclusiveStartTableName=scroll_camel_case,
        Limit=1
    )
    table_name = response['TableNames'][0]
    print(table_name)
    return table_name


def update_technique_list(data, table, file_url):
    if not data['Variations']:
        data['Variations'] = [file_url]
        put_response = table.put_item(Item=data)
        print(put_response)
    else:
        data = update_variations(data, file_url)
        update_response = table.put_item(Item=data)
        print(update_response)


def reset_technique_list(data, table):
    if data['Variations']:
        data['Variations'] = []
        put_response = table.put_item(Item=data)
        print(put_response)


def handle_danzan_ryu(file_stem, file_url):
    scroll = get_scroll_id(file_stem[0])
    print("scroll: ", scroll)
    table_name = get_db_table_name_for_scroll(scroll)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    data = handle_scroll(scroll, file_stem, table)
    print("final file stem: ", file_stem)
    #
    # Special case to remove technique list
    #
    if file_stem.endswith('z'):
        reset_technique_list(data, table)
    else:
        update_technique_list(data, table, file_url)


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
            handle_danzan_ryu(new_stem, file_url)
        else:
            raise Exception("Invalid video file name: "+file_stem)
    else:
        print("Ingest message detected. Ignoring")
    return
