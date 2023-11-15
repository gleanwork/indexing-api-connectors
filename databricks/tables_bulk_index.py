import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.api import datasources_api
from glean_indexing_api_client.api import documents_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.document_permissions_definition import DocumentPermissionsDefinition
import json
import requests
import time

from concurrent.futures import ThreadPoolExecutor, as_completed

import constants

PAGE_SIZE = 10
DATASOURCE = "databricks"

with open('testdata/catalog.json') as f:
    example_catalog = json.load(f)
with open('testdata/schemas.json') as f:
    example_schemas = json.load(f)
with open('testdata/tables.json') as f:
    example_tables = json.load(f)

FETCH=False

def fetch_all_catalogs():
    if FETCH:
        return constants.send_request("/api/2.1/unity-catalog/catalogs")
    else:
        return example_catalog


def fetch_all_schemas(catalog):
    if FETCH:
        return constants.send_request("/api/2.1/unity-catalog/schemas", params={"catalog_name": catalog["name"]})
    else:
        return example_schemas


def fetch_all_tables(schema):
    if FETCH:
        return constants.send_request("/api/2.1/unity-catalog/tables",
                                      params={"catalog_name": schema["catalog_name"], "schema_name": schema["name"]})
    else:
        return example_tables


def get_document_definition(table):
    """Construct document definition from Wikipedia article"""
    title = table["name"]
    url = f'https://e2-dogfood-ext-glean-staging-1.staging.cloud.databricks.com/explore/data/{table["catalog_name"]}/{table["schema_name"]}/{table["name"]}'
    docid = str(table["metastore_id"])
    return DocumentDefinition(
        datasource=constants.DATASOURCE_NAME,
        object_type="Table",
        id=docid,
        title=title,
        view_url=url,  # should match the url_regex in the datasource config
        body=ContentDefinition(
            mime_type="text/plain",
            text_content=""),
        permissions=DocumentPermissionsDefinition(allow_anonymous_access=True)
    )


def issue_bulk_index_documents_request(
        upload_id,
        datasource,
        documents,
        is_first_page,
        is_last_page):
    """
    Issue a /bulkindexdocuments request
    Fails with an indexing_api.ApiException in case of an error
    """
    document_api = documents_api.DocumentsApi(api_client)

    document_api.bulkindexdocuments_post(
        BulkIndexDocumentsRequest(
            upload_id=upload_id,
            datasource=datasource,
            documents=documents,
            is_first_page=is_first_page,
            is_last_page=is_last_page,
            force_restart_upload=False
        ))

    print("Bulk indexed %d documents, is_first_page: %s, is_last_page: %s" %
          (len(documents), is_first_page, is_last_page), flush=True)


def crawl_tables(upload_id: str):
    catalogs = fetch_all_catalogs()
    for catalog in catalogs["catalogs"]:
        schemas = fetch_all_schemas(catalog)
        for schema in schemas["schemas"]:
            tables = fetch_all_tables(schema)
            docs = []
            for table in tables.get("tables", []):
                doc = get_document_definition(table)
                docs.add(doc)
            try:
                issue_bulk_index_documents_request(
                    upload_id=upload_id,
                    datasource=DATASOURCE,
                    documents=docs,
                    is_first_page=False,
                    is_last_page=False)
            except indexing_api.ApiException as e:
                print("Exception while bulk indexing documents: %s\n" % e.body)
                exit(1)


def main():
    crawl_tables("test-id")


if __name__ == "__main__":
    main()
