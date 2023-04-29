import boto3
from datetime import datetime, timedelta
import time
import json
import os
from urllib.parse import urlparse
from pathlib import Path


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


def lambda_handler(event, context):
    if "Complete" in event['Records'][0]['Sns']['Subject']:
        print("Complete detected")
        print(event)
        file_url = json.loads(event['Records'][0]['Sns']['Message'])['hlsUrl']
        print(file_url)
        file_name = urlparse(file_url).path.split('/')[-1]
        print(file_name)
        file_stem = Path(file_name).stem
        print(file_stem)
        if file_stem.startswith('d'):
            print("art: danzan ryu")
            new_stem = remove_char(file_stem, 0)
            print(new_stem)
            scroll = get_scroll_id(new_stem[0])
            print("scroll: ",scroll)
            item_stem = remove_char(new_stem, 0)
            print(item_stem)




        else:
            raise Exception("Invalid video file name!")
    else:
        print("Not complete")
        return
