"""
This script will be used to decipher the VOD files that are stored in the S3 bucket.
"""
import json
import re
from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.dynamodb.conditions import Attr

REMOVE_ALL_TECHNIQUE_VARIATIONS = 'z'


def remove_char(a_str, n):
    """
    Remove a character from a string

    :param a_str: The string to remove the character from
    :param n: The index of the character to remove
    :return: The string with the character removed
    """
    first_part = a_str[:n]
    last_part = a_str[n + 1:]
    return first_part + last_part


def get_scroll_id(scroll_char):
    """
    Identify the scroll we are processing by using a single character
    which was embedded in the file name

    :param scroll_char: A single character from the file name
    :return: the scroll name
    """
    scroll_dict = {
        'n': 'basic_nage',
        'g': 'goshin',
        's': 'shime',
        't': 'basic_stick',
        'f': 'basic_knife',
        'u': 'basic_handgun',
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


def get_weapon_id(weapon_char):
    """
    Identify the weapon we are processing by using a single character
    which was embedded in the file name

    :param weapon_char: A single character from the file name
    :return: the weapon name
    """
    weapon_dict = {
        't': 'stick',
        'f': 'knife',
        'u': 'handgun',
        'r': 'rifle'
    }
    return weapon_dict[weapon_char]


def get_kdm_id(kdm_char):
    """
    Identify the KDM we are processing by using a single character
    which was embedded in the file name

    :param kdm_char: A single character from the file name
    :return: the KDM name
    """
    kdm_dict = {
        'p': 'punch',
        'k': 'kick',
        'i': 'kick_defense',
        'u': 'punch_defense',
    }
    return kdm_dict[kdm_char]


def get_goshin_id(goshin_char):
    """
    Identify the goshin we are processing by using a single character
    which was embedded in the file name

    :param goshin_char: A single character from the file name
    :return: the goshin name
    """
    goshin_dict = {
        'i': 'inside',
        'o': 'outside'
    }
    return goshin_dict[goshin_char]


def convert_to_camel_case(string):
    """
    Convert a string to camel case

    :param string: The string to convert
    :return: The string in camel case
    """
    return ''.join(x.capitalize() or '_' for x in string.split('_'))


def get_stub(file_url):
    """
    Get the stub from a file name

    :param file_url: The file name
    :return: The stub
    """
    file_name = urlparse(file_url).path.split('/')[-1]
    print(file_name)
    return Path(str(file_name)).stem


def sort_url_by_stub(url_list):
    """
    Sort the list of urls by the stub

    :param url_list: The list of urls
    :return: The sorted list of urls
    """
    url_list.sort(key=get_stub)
    return url_list


def handle_simple_table_model(stem, table):
    """
    Handle the case where the file name is a simple stub and the db table
    just uses a number to identify the entry

    :param stem: The file name
    :param table: The DynamoDB table
    :return: The data dictionary
    """
    print(stem)
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
    """
    Handle the basic yawara list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_advanced_yawara(file_stem, ddb_table):
    """
    Handle the advance yawara list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_exercises(file_stem, ddb_table):
    """
    Handle the exercises list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_basic_weapons(file_stem, ddb_table):
    """
    Handle the basic weapons lists

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    item_stem = remove_char(file_stem, 0)
    set_number = item_stem[0]
    print(set_number)
    technique_number = item_stem[1]
    print(technique_number)
    response = ddb_table.scan(
        FilterExpression=Attr('Set').eq(set_number) & Attr('Number').eq(technique_number))
    data = response['Items'][0]
    print(data)
    return data


def handle_basic_stick(file_stem, ddb_table):
    """
    Handle the basic stick list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_basic_weapons(file_stem, ddb_table)


def handle_basic_knife(file_stem, ddb_table):
    """
    Handle the basic knife list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_basic_weapons(file_stem, ddb_table)


def handle_basic_handgun(file_stem, ddb_table):
    """
    Handle the basic handgun list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_basic_weapons(file_stem, ddb_table)


def handle_advanced_weapons(file_stem, ddb_table):
    """
    Handle the advanced weapons list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    weapon = get_weapon_id(file_stem[1])
    print(weapon)
    number = file_stem[2]
    response = ddb_table.scan(
        FilterExpression=Attr('Weapon').eq(weapon) & Attr('Number').eq(number))
    data = response['Items'][0]
    print(data)
    return data


def handle_kdm(file_stem, ddb_table):
    """
    Handle the kdm list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    drill_type = get_kdm_id(file_stem[1])
    print(drill_type)
    number = file_stem[2]
    response = ddb_table.scan(
        FilterExpression=Attr('DrillType').eq(drill_type) & Attr('Number').eq(number))
    data = response['Items'][0]
    print(data)
    return data


def handle_oku(file_stem, ddb_table):
    """
    Handle the oku list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_shime(file_stem, ddb_table):
    """
    Handle the shime list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    flow_number = file_stem[1]
    print(flow_number)
    technique_letter = file_stem[2]
    print(technique_letter)
    response = ddb_table.scan(FilterExpression=Attr('GroundFlowNumber').eq(flow_number) &
                                               Attr('Letter').eq(technique_letter))
    data = response['Items'][0]
    print(data)
    return data


def handle_shinin(file_stem, ddb_table):
    """
    Handle the shinin list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_goshin(file_stem, ddb_table):
    """
    Handle the goshin list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    entering_direction = get_goshin_id(file_stem[1])
    number = file_stem[2]
    response = ddb_table.scan(
        FilterExpression=Attr('Enter').eq(entering_direction) & Attr('Number').eq(number))
    data = response['Items'][0]
    print(data)
    return data


def handle_basic_nage(file_stem, ddb_table):
    """
    Handle the nage list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_advanced_nage(file_stem, ddb_table):
    """
    Handle the advanced nage list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_aikijutsu_nage(file_stem, ddb_table):
    """
    Handle the aikijutsu nage list

    :param file_stem: File name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The data dictionary
    """
    return handle_simple_table_model(file_stem, ddb_table)


def handle_scroll(scroll, file_stem, ddb_table):
    """
    Handle scroll processing.  Each scroll is a string that indicates the
    way it should be processed.

    :param scroll: The scroll name
    :param file_stem: The file name fragment that dictates how the db is updated
    :param ddb_table: The DynamoDB table
    :return: The processed data dictionary
    """
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
    elif scroll == 'basic_stick':
        print("basic stick")
        data = handle_basic_stick(file_stem, ddb_table)
    elif scroll == 'basic_knife':
        print("basic knife")
        data = handle_basic_knife(file_stem, ddb_table)
    elif scroll == 'basic_handgun':
        print("basic handgun")
        data = handle_basic_handgun(file_stem, ddb_table)
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
        raise Exception("Unknown scroll: " + scroll)

    return data


def get_db_table_name_for_scroll(scroll_name):
    """
    Given a scroll name, return the name of the DynamoDB table that contains
    information about the scroll.

    :param scroll_name: The scroll name
    :return: The name of the DynamoDB table that contains information about the scroll.
    """
    #
    # Lengthen the scroll name so that there is no chance of small names like 'oku' or 'kdm'
    # being accidentally found in the random chars in a different table name.
    long_scroll_name = scroll_name + "_model"
    #
    scroll_camel_case = convert_to_camel_case(long_scroll_name)
    print(scroll_camel_case)
    ddb_client = boto3.client('dynamodb')
    response = ddb_client.list_tables(
        ExclusiveStartTableName=scroll_camel_case,
        Limit=1
    )
    table_name = response['TableNames'][0]
    print(table_name)
    return table_name


def extract_stub_list(variations):
    """
    Extract the list of file stubs from the list of urls in variations list

    :param variations: The variations
    :return: The list of stubs
    """
    stub_list = []
    for variation in variations:
        print(variation)
        stub = get_stub(variation)
        stub_list.append(stub)
    return stub_list


def update_technique_list(data, table, file_url):
    """
    Update the technique list in the DynamoDB table

    :param data: The data dictionary
    :param table: The DynamoDB table
    :param file_url: The file URL to add to the list of variations in the data dictionary.
    :return: Nothing
    """

    # data = update_variations(data, file_url)
    new_stub = get_stub(file_url)
    stub_list = extract_stub_list(data['Variations'])
    if new_stub in stub_list:
        print(f'Updating existing entry: {new_stub}')
        for i, stub in enumerate(stub_list):
            if stub == new_stub:
                data['Variations'][i] = file_url
    else:
        print(f'Adding new entry: {new_stub}')
        data['Variations'].append(file_url)

    data['Variations'] = sort_url_by_stub(data['Variations'])
    put_response = table.put_item(Item=data)
    print(put_response)


def reset_technique_list(data, table):
    """
    Special case. Remove the technique list in the DynamoDB table.
    Used for testing purposes.

    :param data: The data dictionary
    :param table: The DynamoDB table
    :return: Nothing
    """
    data['Variations'] = []
    put_response = table.put_item(Item=data)
    print(put_response)


def handle_danzan_ryu(file_stem, file_url):
    """
    Handle the danzan ryu list

    :param file_stem: The file fragment that dictates how the db is updated
    :param file_url: The file URL to add to the list of variations in the data dictionary
    :return:
    """
    scroll = get_scroll_id(file_stem[0])
    print("scroll: ", scroll)
    table_name = get_db_table_name_for_scroll(scroll)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    data = handle_scroll(scroll, file_stem, table)
    print("final file stem: ", file_stem)
    if file_stem.endswith(REMOVE_ALL_TECHNIQUE_VARIATIONS):
        reset_technique_list(data, table)
    else:
        update_technique_list(data, table, file_url)


def lambda_handler(event, context):
    """
    Lambda function handler

    :param event: The Lambda event
    :param context: The Lambda context
    :return: Nothing
    """
    if "Complete" in event['Records'][0]['Sns']['Subject']:
        print("'Complete' type notification detected")
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
            raise Exception("Invalid video file name: " + file_stem)
    else:
        print("'Ingest' type message detected. Ignoring")
    return
