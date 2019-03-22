#!/usr/bin/env python3
import requests
import requests.exceptions
import os, time, json, datetime
from datadog import initialize

CONNECT_HOST = 'localhost:8083'
CONNECTOR_UNIQUE_NAME = os.path.expandvars(os.getenv('CONNECTOR_UNIQUE_NAME'))
CONNECTOR_CONFIG_JSON = os.getenv('CONNECTOR_CONFIG_JSON')
CONNECTOR_CONFIG_JSON_PLAIN = os.path.expandvars(CONNECTOR_CONFIG_JSON)

###################### Datadog config ######################
dd_tags = ['env:{{ .Values.ENV }}','app_name:{{ include "kafka-connect-helm.fullname" . }}']
dd_options = {
    'statsd_host': os.getenv('DATADOG_HOST')
}
initialize(**dd_options)

def health_check():
    try:
        r = requests.get('http://{0}'.format(CONNECT_HOST), timeout=10)
        r.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xxx
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        print("Connector Service is not ready yet")
        return -1
    except requests.exceptions.HTTPError:
        print("HTTPError 4xx, 5xx")
        return r.status_code
    else:
        print("Connector service is up!")
        return r.status_code

def delete_config():
    try:
        print("Deleting the existing config of {0}".format(CONNECTOR_UNIQUE_NAME))
        r = requests.delete('http://{0}/connectors/{1}'.format(CONNECT_HOST, CONNECTOR_UNIQUE_NAME))
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print("time: {0}, err: {1}".format(datetime.datetime.now(),err))
    return r.status_code

def create_config():
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
    }
    payload = {
    'name': "{0}".format(CONNECTOR_UNIQUE_NAME),
    'config': json.loads("{0}".format(CONNECTOR_CONFIG_JSON_PLAIN))
    }
    try:
        print("Creating config of {0}".format(CONNECTOR_UNIQUE_NAME))
        r = requests.post('http://{0}/connectors/'.format(CONNECT_HOST), headers=headers, data=json.dumps(payload))
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print("time: {0}, err: {1}".format(datetime.datetime.now(),err))
    return r.status_code

def get_all_connector_names():
    connector_names = None
    try:
        print("Getting all the existing configs")
        r = requests.get('http://{0}/connectors'.format(CONNECT_HOST))
        r.raise_for_status()
        assert (r.status_code == 200), 'connector API does not return 200'
        connector_names = r.json()
        assert (type(connector_names) == list), "response json is not a list!"
    except requests.exceptions.HTTPError as err:
        print(err)
    return r.status_code, connector_names

def task_checker(connector_names):
    from datadog import statsd
    try:
        for connector in connector_names:
            r = requests.get('http://{0}/connectors/{1}/status'.format(CONNECT_HOST, connector))
            r.raise_for_status()
            assert (r.status_code == 200), 'connector API does not return 200'
            connector_status = r.json()
            connector_state = connector_status.get('connector').get('state')
            if connector_state != 'RUNNING':
                print("connector {0} is down, restarting the connector".format(connector))
                r = requests.post('http://{0}/connectors/{1}/restart'.format(CONNECT_HOST, connector))
                r.raise_for_status()
                #assert (r.status_code == 200), 'connector API does not return 200'
                statsd.gauge('kafka_connect.num_running_tasks', 0, tags=dd_tags)
            else:
                tasks = connector_status.get('tasks')
                print("{0} has num of running of tasks : {1}".format(connector, len(tasks)))
                statsd.gauge('kafka_connect.num_running_tasks', len(tasks), tags=dd_tags)
                for task in tasks:
                    task_state = task.get('state')
                    if task_state != 'RUNNING':
                        print("task {0} of the connector {1} is down, restarting the task".format(task.get('id'), connector))
                        r = requests.post('http://{0}/connectors/{1}/tasks/{2}/restart'.format(CONNECT_HOST, connector, task.get('id')))
                        r.raise_for_status()
                        #assert (r.status_code == 200), 'connector API does not return 200'
    except requests.exceptions.HTTPError as err:
        print(err)
    return r.status_code

def check_tasks():
    status_code, connector_names = get_all_connector_names()
    if len(connector_names) == 0:
        create_config()
    else:
        task_checker(connector_names)

def main():
    while health_check() != 200:
        time.sleep(10)
    # map the inputs to the function blocks
    actions = {
        'check_tasks' : check_tasks,
        'delete_config' : delete_config
    }

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", type=str,
        choices=['check_tasks','delete_config'],
        help="positional argument define the action type")
    args = parser.parse_args()

    action_func = actions.get(args.action, lambda: "nothing")
    action_func()

if __name__== "__main__":
    main()