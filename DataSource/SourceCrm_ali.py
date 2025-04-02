from DataSource.Source_Hub import Source_Hub
import pandas as pd
import os
import configparser
import requests

# Salesforce API configuration
API_VERSION = 'v61.0'
PROXIES = {}


class SourceCrm_ali(Source_Hub):
    def __init__(self, table_name, filter_rule=None):
        # 初始化方法
        self.table_name = table_name
        self.filter = filter_rule

    @staticmethod
    def get_access_token(payload):

        response = requests.post(payload['Token_URL'],
                                 payload,
                                 verify=False,
                                 proxies=PROXIES)

        if response.status_code == 200:
            response_json = response.json()
            access_token = response_json.get('access_token')
            instance_url = response_json.get('instance_url')
            return access_token, instance_url
        else:
            raise Exception(f"Failed to obtain access token: {response.text}")

    # 获取数据
    def get_data(self):
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'env', 'crm_ali.ini'))
        payload = {
            'grant_type': 'password',
            'client_id': config['qa']['Client_ID'],
            'client_secret': config['qa']['Client_Secret'],
            'username': config['qa']['Username'],
            'password': config['qa']['Password'] + config['qa']['Security_Token'],
            'Token_URL': config['qa']['Token_URL']
        }
        try:
            access_token, instance_url = self.get_access_token(payload)

            # Define the API endpoint for retrieving accounts
            api_endpoint = f'{instance_url}/services/data/{API_VERSION}/sobjects/{self.table_name}/describe'
            # describe_url = f'{instance_url}/services/data/{API_VERSION}/sobjects/{self.table_name}/describe'
            query_url = f'{instance_url}/services/data/{API_VERSION}/query'

            # Set the headers with the access token
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            # Make a GET request to the Salesforce API
            response = requests.get(api_endpoint, headers=headers, verify=False, proxies=PROXIES)

            # Check if the request was successful
            if response.status_code == 200:
                description = response.json()
                fields = [field['name'] for field in description['fields'] if field['name'] != 'attributes']
                query = f"SELECT {', '.join(fields)} FROM {self.table_name} {self.filter}"
                print(query)
            else:
                raise Exception("Failed to retrieve accounts:", response.status_code, response.text)
            query_response = requests.get(query_url, headers=headers, params={'q': query}, verify=False, proxies=PROXIES)
            if query_response.status_code == 200:
                records = query_response.json().get('records', [])
                for record in records:
                    if 'attributes' in record:
                        del record['attributes']
                columns = list(records[0].keys()) if records else []
                data = [list(record.values()) for record in records]
                df = pd.DataFrame(data, columns=columns)
                return df
            else:
                raise Exception(f"Failed to query Salesforce: {response.text}")

        except Exception as e:
            raise Exception(f"Error occurred while retrieving data: {str(e)}")



