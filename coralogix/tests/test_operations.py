# Edit the config_and_params.json file and add the necessary parameter values.
# Ensure that the provided input_params yield the correct output schema.
# Add logic for validating conditional_output_schema or if schema is other than dict.
"""
Copyright start
MIT License
Copyright (c) 2024 Fortinet Inc
Copyright end
"""

import os
import sys
import json
import pytest
import inspect
import logging
import requests
import importlib
from connectors.core.connector import ConnectorError

current_directory = os.path.dirname(os.path.abspath(__file__))
connector_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
connectors_dir = os.path.abspath(os.path.join(connector_directory, os.pardir))
sys.path.insert(0, str(connectors_dir))

config_file_path = os.path.join(current_directory, 'config_and_params.json')
with open(config_file_path, 'r') as file:
    params = json.load(file)

info_json_path = os.path.join(connector_directory, 'info.json')
with open(info_json_path, 'r') as file:
    info_json = json.load(file)

connector_dir_name = os.path.basename(connector_directory)
connector_module = importlib.import_module(connector_dir_name+'.connector')
connector_classes = inspect.getmembers(connector_module, inspect.isclass)
for name, cls in connector_classes:
    if cls.__module__ == connector_dir_name+'.connector':
        connector_instance = cls(info_json=info_json)

logger = logging.getLogger(__name__)
    

def run_success_test(cache, config, operation_name, action_params):
    logger.info("params: {0}".format(action_params))
    result = connector_instance.execute(config, operation_name, action_params)
    logger.info(result)
    assert result
    cache.set(operation_name+'_response', result)


def run_output_schema_validation(cache, config, operation_name):
    schema = {}
    for operation in info_json.get("operations"):
        if operation.get('operation') == operation_name:
            if "conditional_output_schema" in operation or "api_output_schema" in operation:
                pytest.skip("Skipping test because conditional_output_schema or api_output_schema is not supported.")
            else:
                schema = operation.get('output_schema')
            break
    resp = cache.get(operation_name+'_response', None)
    if isinstance(schema, dict) and isinstance(resp, dict):
        logger.info("output_schema: {0} \n API_response: {1}".format(schema, resp))
        assert resp.keys() == schema.keys()
    else:
        pytest.skip("Skipping test because output_schema is not a dict.")
    

def run_invalid_config_test(config, param_name, param_type):
    config[param_name] = params.get('invalid_params')[param_type]
    with pytest.raises(ConnectorError):
        result = connector_instance.check_health(config)
        logger.info(result)


def run_invalid_param_test(config, operation_name, param_name, param_type):
    input_params = params.get(operation_name)[0].copy()
    input_params[param_name] = params.get('invalid_params')[param_type]
    with pytest.raises(ConnectorError):
        result = connector_instance.execute(config, operation_name, input_params)
        logger.info(result)


# To test with different configuration values, update the code below.
@pytest.fixture(scope="session")
def valid_configuration():
    conn_config = params.get('config')[0].copy()
    connector_label = info_json.get('label')
    connector_name = info_json.get('name')
    connector_version = info_json.get('version')
    get_connectors = requests.request('GET', f'http://localhost:8000/integration/connectors?search={connector_label}')
    connectors = get_connectors.json()
    connector_id = ''
    if connectors.get('totalItems') > 0:
        for connector in connectors.get('data'):
            if connector.get('name') == connector_name and connector.get('version') == connector_version:
                connector_id = connector.get('id')
    payload = {
        "connector": connector_id,
        "name": "pytest_config",
        "config": conn_config
    }
    add_config = requests.request('POST', f'http://localhost:8000/integration/configuration/?format=json', json=payload)
    config = add_config.json()
    delete_config = config.get('id')
    params.get('config')[0].update({"config_id": config.get('config_id')})
    yield params.get('config')[0]
    requests.request('DELETE', f'http://localhost:8000/integration/configuration/{delete_config}/?format=json')
    
    
@pytest.fixture(scope="session")
def valid_configuration_with_token(valid_configuration):
    config = valid_configuration.copy()
    connector_instance.check_health(config)
    return config
    

@pytest.mark.check_health     
def test_check_health_success(valid_configuration):
    config = valid_configuration.copy()
    result = connector_instance.check_health(config) 
    logger.info(result)
    

@pytest.mark.check_health     
def test_check_health_invalid_server_url(valid_configuration):
    run_invalid_config_test(config=valid_configuration.copy(), param_name='server_url', param_type='text')
    

@pytest.mark.check_health     
def test_check_health_invalid_api_key(valid_configuration):
    run_invalid_config_test(config=valid_configuration.copy(), param_name='api_key', param_type='password')
    

@pytest.mark.search_archived_logs
@pytest.mark.parametrize("input_params", params['search_archived_logs'])
def test_search_archived_logs_success(cache, valid_configuration_with_token, input_params):
    run_success_test(cache, config=valid_configuration_with_token.copy(), operation_name='search_archived_logs',
                     action_params=input_params.copy())
  

@pytest.mark.search_archived_logs
def test_validate_search_archived_logs_output_schema(cache, valid_configuration_with_token):
    run_output_schema_validation(cache, config=valid_configuration_with_token.copy(),
                                 operation_name='search_archived_logs')


@pytest.mark.search_archived_logs     
def test_search_archived_logs_invalid_query(valid_configuration_with_token):
    run_invalid_param_test(config=valid_configuration_with_token.copy(), operation_name='search_archived_logs', 
                           param_name='query', param_type='text')
    
