import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.api import datasources_api
from glean_indexing_api_client.api import documents_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.document_permissions_definition import DocumentPermissionsDefinition
from constants import send_request, DATASOURCE_NAME, DASHBOARD_OBJECT_NAME, BASE_URL, API_CLIENT
from typing import List
import os

def crawl_notebooks(upload_id: str, rootFolder: str):
    remainingDirs = [rootFolder]
    while(remainingDirs):
        notebooks = [] 
        currentDir = remainingDirs.pop()
        params = {
            'path': currentDir
        }
        response = send_request("/api/2.0/workspace/list", params=params)
        for asset in response["objects"]:
            asset_type = asset["object_type"]
            if(asset_type == "DIRECTORY"):
                remainingDirs.append(asset)
            elif(asset_type == "NOTEBOOK"):
                asset["html"] = get_notebook_html(asset["path"])
                notebooks.append(asset)
        upload_notebooks(notebooks, upload_id)

def get_notebook_html(path):
    params = {
        'path': path,
        'format': 'SOURCE',
        'direct_download': 'true',
    }
    response = send_request("/api/2.0/workspace/export", params=params)
    return response

def upload_notebooks(notebooks: List[dict], upload_id: str):
    """Upload dashboards to Glean"""
    documents = []
    
    # Convert notebooks to document representations
    for notebook in notebooks:
        documents.append(
            DocumentDefinition(
                datasource=DATASOURCE_NAME,
                object_type=DASHBOARD_OBJECT_NAME,
                id=str(notebook['object_id']),
                title=os.path.basename(notebook['path']),
                view_url=f'{BASE_URL}/sql/notebooks/{notebook["object_id"]}',
                body=ContentDefinition(
                    mime_type='text/plain',
                    text_content=notebook['html']),
                # TODO: Add permissions via dashboard's ACL from https://docs.databricks.com/api/workspace/dbsqlpermissions/get
                permissions=DocumentPermissionsDefinition(allow_anonymous_access=True)
            )
        )
    document_api = documents_api.DocumentsApi(API_CLIENT)

    document_api.bulkindexdocuments_post(
        BulkIndexDocumentsRequest(
            upload_id=upload_id,
            datasource=DATASOURCE_NAME,
            documents=documents,
            is_first_page=False,
            is_last_page=False,
            force_restart_upload=False
        ))

    logging.info("Bulk indexed %d dashboards" %
          (len(documents)))

def list_notebooks():
    # Send a request to list notebooks
    params = {
        'path': '/Users/alexis.deschamps@databricks.com/example_folder_1'
    }
    response = send_request("/api/2.0/workspace/list", params=params)
    print(response)

    notebooks = response["objects"]
    
    for notebook in notebooks:
        print(notebook["path"])

        
crawl_notebooks('1','/Users/alexis.deschamps@databricks.com/example_folder_1')