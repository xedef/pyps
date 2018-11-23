#!/usr/bin/env python

"""
    pyps
    ~~~~

    Python AWS SSM Parameter Store writer
"""
import json
import os
from datetime import datetime
from os.path import (expanduser, normpath,)
import argparse
import boto3

# used for backups and projects
PROJECT_FOLDER_NAME = '.pyps'
ENRIVONMENTS = ['development', 'staging', 'production']
SSM_VALUE_LIMIT = 4096

YELLOW = '\033[1;33;40m'
WHITE = '\033[0;37;40m'
RED = '\033[0;31;40m'
GREEN = '\033[0;32;40m'
CYAN = '\033[0;36;40m'

COLUMN_SPACING = 35

def ct(text, color=WHITE):
    return '{}{}{}'.format(color, text, WHITE)

def print_message(message, color=WHITE):
    print(ct(message, color))

def safe_get(array, index):
    try:
        return array[index]
    except Exception as error:
        return ''


def get_summary_table(old, new):
    old_keys = old.keys()
    new_keys = new.keys()

    missing = sorted(set(old_keys).difference(new_keys))
    added = sorted(set(new_keys).difference(old_keys))
    changed = []
    unchanged = []

    common_keys = sorted(set(old_keys) & set(new_keys))
    for key in common_keys:
        if old[key] == new[key]:
            unchanged.append(key)
        else:
            changed.append(key)
    lines = [
        ct(
            '{}{}{}{}'.format(
                'Deleted keys'.ljust(COLUMN_SPACING),
                'Added keys'.ljust(COLUMN_SPACING),
                'Changed keys'.ljust(COLUMN_SPACING),
                'Unchanged keys'
            ),
            CYAN
        )
    ]
    lines.append(ct('='*COLUMN_SPACING*4, CYAN))
    limit = max(len(missing), len(added), len(changed), len(unchanged))
    for ix in range(0, limit):
        lines.append(
            '{}{}{}{}'.format(
                ct(safe_get(missing, ix).ljust(COLUMN_SPACING), RED),
                ct(safe_get(added, ix).ljust(COLUMN_SPACING), GREEN),
                ct(safe_get(changed, ix).ljust(COLUMN_SPACING), YELLOW),
                ct(safe_get(unchanged, ix), WHITE),
            )
        )
    return lines


def retrieve(ssm, path):
    print_message('Retrieving parameters from "{}"'.format(path))
    def query_ssm(ssm, parameters=[], next_token=None):
        invoke_params = {
            'Path': path,
            'MaxResults': 10
        }
        if next_token is not None:
            invoke_params['NextToken'] = next_token

        response = ssm.get_parameters_by_path(**invoke_params)
        parameters = parameters + response.get('Parameters', [])
        next_token = response.get('NextToken', None)

        if next_token is not None:
            parameters = query_ssm(ssm, parameters=parameters, next_token=next_token)
        return parameters

    chunks = query_ssm(ssm)

    if not chunks:
        return {}, []

    chunks.sort(key=lambda item: item.get('Name'))
    consolidated_str = str()
    keys = []

    for chunk in chunks:
        consolidated_str += chunk.get('Value')
        keys.append(chunk.get('Name'))

    consolidated = {}
    try:
        consolidated = json.loads(consolidated_str)
    except Exception as e:
        fail_and_die(str(e))

    return consolidated, keys

def confirm_or_die(message):
    result = input(message + '\n')
    if result[:1].lower() != 'y':
        print_message('Bye\n', CYAN)
        exit()

def fail_and_die(message):
    print_message('\n{}\n'.format(message), RED)
    exit()

def backup(parameters, project_name):
    home_path = expanduser('~')
    today = datetime.now()
    backup_name = '{}-{}.bak.json'.format(project_name, today.strftime('%Y%m%d_%H%M%S'))
    backup_dir = normpath('{}/{}/backups/{}/'.format(home_path, PROJECT_FOLDER_NAME, project_name))

    try:
        os.makedirs(backup_dir, exist_ok=True)
    except Exception as error:
        fail_and_die(str(error))

    backup_path = normpath('{}/{}'.format(backup_dir, backup_name))

    with open(backup_path, 'w') as backup_file:
        backup_file.write(json.dumps(parameters, indent=2))

    print_message('Backup written at {}'.format(backup_path), GREEN)


def chunkenize(str):
    string_length = len(str)
    return [ str[i:i + SSM_VALUE_LIMIT] for i in range(0, string_length, SSM_VALUE_LIMIT) ]


def write(ssm, path, new_parameters):
    stringified = ''
    try:
        stringified = json.dumps(new_parameters)
    except Exception as error:
        fail_and_die(str(error))

    chunks = chunkenize(stringified)
    written = []
    try:
        for index, chunk in enumerate(chunks):
            response = ssm.put_parameter(
                Name='{}/part_{}'.format(path, index),
                Value=chunk,
                Type='String'
            )
            written.append(response)
    except Exception as error:
        fail_and_die(str(error))

    return written

def load_new_parameters(environment):
    current_path = os.path.dirname(os.path.abspath(__file__))
    infile = './{}/{}.json'.format(PROJECT_FOLDER_NAME, environment)
    try:
        with open(infile, 'r') as file_handle:
            return json.load(file_handle)
    except Exception as error:
        fail_and_die('Could not parse input file "{}"\n{}'.format(infile, str(error)))

def run():
    parser = argparse.ArgumentParser(description='Validates, minifies and escapes a JSON input.')
    parser.add_argument('--environment', '-e', required=True, type=str, choices=ENRIVONMENTS, help='Environment being updated')
    parser.add_argument('--project', '-p', required=True, type=str, nargs='?', help='Project that config is set to')

    args = parser.parse_args()
    environment = args.environment
    project_name = args.project.strip('/')

    json_contents = load_new_parameters(environment)

    path = '/{}/{}'.format(environment, project_name)

    ssm = boto3.client('ssm')

    parameters, parameters_names = retrieve(ssm, path)
    project_found = len(parameters_names) > 0

    # if the project was not found, confirm that the user wants to add it as new
    message = 'Replace existing configuration? (y/N)'
    if not project_found:
        message = 'Configuration was not found for project {}{}{}. Add project? (y/N)'.format(YELLOW, path, WHITE)

    confirm_or_die(message)

    lines = get_summary_table(parameters, json_contents)

    print('\nFollowing changes will be applied:')
    for line in lines:
        print(line)

    confirm_or_die(ct('\nProceed? (y/N)', YELLOW))

    if project_found:
        backup(parameters, project_name)
        ssm.delete_parameters(
            Names=parameters_names
        )

    written_keys = write(ssm, path, json_contents)
    print_message('{} keys were written'.format(len(written_keys)), GREEN)


if __name__ == '__main__':
    run()