from glean_indexing_api_client.api import documents_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.document_permissions_definition import DocumentPermissionsDefinition
from glean_indexing_api_client.model.user_reference_definition import UserReferenceDefinition

import json
import requests
import time
import logging
from datetime import datetime, timezone

logging.getLogger().setLevel(logging.INFO)
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from constants import send_request, DATASOURCE_NAME, DASHBOARD_OBJECT_NAME, BASE_URL, API_CLIENT

PAGE_SIZE = 10

def _parse_epoch_from_datetime_string(datetime_string: str):
    fmt = "%Y-%m-%dT%H:%M:%SZ" 

    # Create a datetime object from your string
    dt = datetime.strptime(datetime_string, fmt)
    # Convert datetime object to seconds since the Unix Epoch
    epochseconds = int(dt.replace(tzinfo=timezone.utc).timestamp())
    return epochseconds
    # epoch_seconds = int(time.mktime(dt.timetuple(), tz=pytz.UTC))
    # return epoch_seconds

def _upload_dashboards(dashboards: List[dict], upload_id: str):
    """Upload dashboards to Glean"""
    documents = []
    
    # Convert dashboards to document representations
    for dashboard in dashboards:
        # Stitch content from dashboard's widgets
        stitched_content = [dashboard['name']]
        if dashboard['widgets']:
            # TODO: Consider supporting further details in the widget; such as query details for a visualization
            for widget in dashboard['widgets']:
                options = widget['options']
                if 'title' in options:
                    stitched_content.append(options['title'])
                if 'description' in options:
                    stitched_content.append(options['description'])
                if 'visualization' in widget:
                    visualization = widget['visualization']
                    stitched_content.append(visualization['name'])
                    stitched_content.append(visualization['description'])

        documents.append(
            DocumentDefinition(
                datasource=DATASOURCE_NAME,
                object_type=DASHBOARD_OBJECT_NAME,
                id=dashboard['id'],
                title=dashboard['name'],
                created_at=_parse_epoch_from_datetime_string(dashboard['created_at']),
                updated_at=_parse_epoch_from_datetime_string(dashboard['updated_at']),
                author=UserReferenceDefinition(
                    datasource_user_id=str(dashboard['user_id']),
                ),
                tags=dashboard['tags'],
                view_url=f'{BASE_URL}/sql/dashboards/{dashboard["id"]}',
                body=ContentDefinition(
                    mime_type='text/plain',
                    text_content=' '.join(stitched_content)),
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



def _fetch_dashboard(dashboard_id: str):
    return send_request(f'/api/2.0/preview/sql/dashboards/{dashboard_id}')

def crawl_dashboards(upload_id: str):
    page = 1

    # TODO: Proper checkpointing
    while True:
        response = send_request('/api/2.0/preview/sql/dashboards')
        results = response['results']
        if len(results) == 0:
            logging.info(f'Empty dashboards response for page {page}. Finishing crawl')
            break
        logging.info(f'Listed {len(results)} dashboards on page {page}')
        # We need to fetch the individual dashboards to get their widgets; otherwise widgets shows up as None
        fetched_dashboards = [_fetch_dashboard(dashboard['id']) for dashboard in results]

        _upload_dashboards(fetched_dashboards, upload_id)

        if response['count'] < response['page_size']:
            logging.info(f'Unfilled dashboard page {page}. Finishing crawl')
            break
        else:
            page += 1
