import glean_indexing_api_client as indexing_api
import os
import requests
from dotenv import load_dotenv

load_dotenv()

DATASOURCE_NAME = "lakehousehackathon"
DASHBOARD_OBJECT_NAME = "Dashboard"
NOTEBOOK_OBJECT_NAME = "Notebook"

_configuration = indexing_api.Configuration(
    host="https://databricks-be.glean.com/api/index/v1",
    access_token=os.getenv("GLEAN_API_KEY"))

BASE_URL = "https://e2-dogfood-ext-glean-staging-1.staging.cloud.databricks.com"
API_CLIENT = indexing_api.ApiClient(_configuration)


def send_request(endpoint: str, params=None, data=None, method='GET'):
    headers = {}

    # Add bearer authorization if token is provided
    headers['Authorization'] = 'Bearer ' + str(os.getenv('DATABRICKS_TOKEN'))

    url = BASE_URL.removesuffix('/') + '/' + endpoint.removeprefix('/')

    # Initiate HTTP request based on method
    if method == 'GET':
        response = requests.get(url, headers=headers, params=params)
    elif method == 'POST':
        response = requests.post(url, headers=headers, json=data, params=params)
    elif method == 'PUT':
        response = requests.put(url, headers=headers, json=data, params=params)
    elif method == 'DELETE':
        response = requests.delete(url, headers=headers, params=params)
    else:
        raise ValueError('Invalid method')

    # Throw an error if the request was unsuccessful
    response.raise_for_status()
    try:
        return response.json()
    except:
        return response.text
