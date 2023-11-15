import uuid

from add_datasource import add_datasource
from dashboards_bulk_index import crawl_dashboards
from tables_bulk_index import crawl_tables
from glean_indexing_api_client.api import documents_api
import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from constants import API_CLIENT, DATASOURCE_NAME

def main():
    add_datasource(DATASOURCE_NAME)

    upload_id = str(uuid.uuid4())
    document_api = documents_api.DocumentsApi(API_CLIENT)

    # Begin bulk indexing batch
    document_api.bulkindexdocuments_post(
        BulkIndexDocumentsRequest(
            upload_id=upload_id,
            datasource=DATASOURCE_NAME,
            documents=[],
            is_first_page=True,
            is_last_page=False,
            force_restart_upload=False
        ))

    crawl_dashboards(upload_id=upload_id)
    crawl_tables(upload_id=upload_id)
    
    # Add more bulk indexing requests here; these should not specify is_first_page or is_last_page

    # End bulk indexing batch
    document_api.bulkindexdocuments_post(
        BulkIndexDocumentsRequest(
        upload_id=upload_id,
        datasource=DATASOURCE_NAME,
        documents=[],
        is_first_page=False,
        is_last_page=True,
        force_restart_upload=False
    ))                     




if __name__ == "__main__":
    main()