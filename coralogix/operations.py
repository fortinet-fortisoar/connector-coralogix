""""
Copyright start
MIT License
Copyright (c) 2024 Fortinet Inc
Copyright end
"""
import json
from connectors.core.connector import get_logger, ConnectorError
import requests

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
                elif response.content:
                    return response.json()
                else:
                    return 'custom_response'
            elif response.status_code == 400:
                error_response = response.json()
                error_description = error_response['message'] if error_response.get('message') else error_response
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
    endpoint = '/api/v1/dataprime/query'
    metadata = {
        "startDate": params.get('start_date'),
        "endDate": params.get('end_date'),
        "defaultSource": "logs"
    }
    if isinstance(params.get('metadata'), dict):
        metadata.update(params.get('metadata'))
    metadata = build_payload(metadata)
    payload = {
        'query': params.get('query', ''),
        'metadata': metadata
    }
    result = obj.make_request(endpoint, 'POST', data=json.dumps(payload))
    if result == 'custom_response':
        return {"result": {"results": []}}
    return result


def check_health(config):
    params = {
        'query': 'limit 1'
    }
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


operations = {
    'search_archived_logs': search_archived_logs
}
