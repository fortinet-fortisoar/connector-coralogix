""""
Copyright start
MIT License
Copyright (c) 2024 Fortinet Inc
Copyright end
"""
import json
from connectors.core.connector import get_logger, ConnectorError
import requests
from datetime import datetime, timedelta
from .constants import *

logger = get_logger('coralogix')


class Coralogix:
    def __init__(self, config):
        self.server_url = config.get('server_url').strip('/')
        if not (self.server_url.startswith('http://') or self.server_url.startswith('https://')):
            self.server_url = 'https://' + self.server_url
        self.api_key = config.get('api_key')
        self.verify_ssl = config.get('verify_ssl', True)

    def make_request(self, endpoint, method='get', data=None, params=None, files=None):
        try:
            url = self.server_url + endpoint
            logger.info('Executing url {}'.format(url))
            headers = {
                'Authorization': 'Bearer {0}'.format(self.api_key),
                'Content-Type': 'application/json'
            }
            # CURL UTILS CODE
            try:
                from connectors.debug_utils.curl_script import make_curl
                make_curl(method, url, headers=headers, params=params, data=data, verify_ssl=self.verify_ssl)
            except Exception as err:
                logger.debug(f"Error in curl utils: {str(err)}")

            response = requests.request(method, url, params=params, files=files, data=data, headers=headers,
                                        verify=self.verify_ssl)
            if response.ok:
                logger.info('successfully get response for url {}'.format(url))
                if method.lower() == 'delete':
                    return response
                elif response.text:
                    try:
                        return response.json()
                    except requests.JSONDecodeError as error:
                        logger.warning("Response is not valid json. Error: {}".format(error))
                        return response.text
                else:
                    return 'custom_response'
            elif response.status_code == 400:
                error_description = response.text
                raise ConnectorError({'error_description': error_description})
            elif response.status_code == 401:
                error_response = response.json()
                if error_response.get('error'):
                    error_description = error_response['error']
                else:
                    error_description = error_response['message']
                raise ConnectorError({'error_description': error_description})
            elif response.status_code == 404:
                error_response = response.json()
                if error_response.get('message'):
                    error_description = error_response['message']
                    raise ConnectorError({'error_description': error_description})
                raise ConnectorError(error_response)
            else:
                try:
                    logger.error(response.json())
                    raise ConnectorError(str(response.json()))
                except Exception as err:
                    logger.debug(str(err))
                    error_response = {'status_code': response.status_code,
                                      'message': response.text if response.text else response.reason}
                    logger.error(str(error_response))
                    raise ConnectorError(str(error_response))
        except requests.exceptions.SSLError:
            raise ConnectorError('SSL certificate validation failed')
        except requests.exceptions.ConnectTimeout:
            raise ConnectorError('The request timed out while trying to connect to the server')
        except requests.exceptions.ReadTimeout:
            raise ConnectorError('The server did not send any data in the allotted amount of time')
        except requests.exceptions.ConnectionError:
            raise ConnectorError('Invalid endpoint or credentials')
        except Exception as err:
            raise ConnectorError(str(err))


def search_archived_logs(config, params):
    obj = Coralogix(config)
    metadata = {
        "startDate": params.get('start_date'),
        "endDate": params.get('end_date'),
        "defaultSource": "logs"
    }
    if isinstance(params.get('metadata'), dict):
        metadata.update(params.get('metadata'))
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    if not end_date:
        end_date = get_time_before_hours()
        metadata['endDate'] = end_date
    if not start_date:
        metadata['startDate'] = get_time_before_hours(hours=MAX_TIME_RANGE, from_date=end_date)
    metadata = build_payload(metadata)
    payload = {
        'query': params.get('query', ''),
        'metadata': metadata
    }
    result = obj.make_request(SEARCH_ARCHIVED_LOGS_ENDPOINT, 'POST', data=json.dumps(payload))
    if result == 'custom_response':
        return {"result": {"results": []}}
    if isinstance(result, str):
        return handle_text_response(result)
    return result


def check_health(config):
    params = CHECK_HEALTH_PARAM
    search_archived_logs(config, params)
    return True


def build_payload(params: dict):
    payload = {}
    for k, v in params.items():
        if isinstance(v, dict) and v:
            payload[k] = build_payload(v)
        elif isinstance(v, (int, bool)) or v:
            payload[k] = v
    return payload


def get_time_before_hours(hours=0, from_date=None):
    if not from_date:
        result_time = datetime.utcnow()
    else:
        result_time = datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    if hours:
        result_time -= timedelta(hours=hours)
    formatted_time = result_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return formatted_time


def handle_text_response(result: str, count=0):
    if not result:
        return {}
    if isinstance(result, dict):
        return result
    if count >= MAX_HANDLE_RESPONSE_COUNT:
        return {"partially_parsed_result": result}
    result_list = result.split('\n', maxsplit=1)
    json_result = json.loads(result_list[0]) if result_list[0] else {}
    if len(result_list) > 1:
        text_result = result_list[1]
        result = handle_text_response(text_result, count=count + 1)
        if json_result.get('result') and result.get('result'):
            if json_result.get('result').get('results') and result.get('result').get('results'):
                json_result['result'].get('results').extend(result.get('result').get('results'))
        else:
            json_result.update(result)
    return json_result


operations = {
    'search_archived_logs': search_archived_logs
}
