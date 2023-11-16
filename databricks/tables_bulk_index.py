import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.api import documents_api
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.user_reference_definition import UserReferenceDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.comment_definition import CommentDefinition
from glean_indexing_api_client.model.document_permissions_definition import DocumentPermissionsDefinition
import json
from constants import API_CLIENT
from typing import List

import constants

PAGE_SIZE = 10
FETCH = True


def fetch_all_catalogs():
    if FETCH:
        return constants.send_request("/api/2.1/unity-catalog/catalogs")
    else:
        with open('testdata/catalog.json') as f:
            example_catalog = json.load(f)
        return example_catalog


def fetch_all_schemas(catalog):
    if FETCH:
        return constants.send_request("/api/2.1/unity-catalog/schemas", params={"catalog_name": catalog["name"]})
    else:
        with open('testdata/schemas.json') as f:
            example_schemas = json.load(f)
        return example_schemas


def fetch_all_tables(schema):
    if FETCH:
        return constants.send_request("/api/2.1/unity-catalog/tables",
                                      params={"catalog_name": schema["catalog_name"], "schema_name": schema["name"]})
    else:
        with open('testdata/tables.json') as f:
            example_tables = json.load(f)
        return example_tables


def get_comment_definition(table) -> List[CommentDefinition]:
    comments = []
    for column in table["columns"]:
        comments.append(CommentDefinition(
            id=column["name"],
            content=ContentDefinition(
                mime_type="text/plain",
                text_content=column["name"] + " " + column.get("comment", ""),
            ),
        ))
    return comments

def get_document_definition(table):
    """Construct document definition from Wikipedia article"""
    title = table["name"]
    url = f'https://e2-dogfood-ext-glean-staging-1.staging.cloud.databricks.com/explore/data/{table["catalog_name"]}/{table["schema_name"]}/{table["name"]}'
    docid = str(table["table_id"])
    return DocumentDefinition(
        datasource=constants.DATASOURCE_NAME,
        object_type="Table",
        id=docid,
        title=title,
        view_url=url,  # should match the url_regex in the datasource config
        created_at=int(table["created_at"] / 1000),
        author=UserReferenceDefinition(
            email=table["created_by"],
        ),
        updated_at=int(table["updated_at"] / 1000),
        updated_by=UserReferenceDefinition(
            email=table["updated_by"],
        ),
        body=ContentDefinition(
            mime_type="text/plain",
            text_content=table.get("comment", "")
        ),
        container=table["schema_name"],
        containerObjectType="schema",
        comments=get_comment_definition(table),
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
    document_api = documents_api.DocumentsApi(API_CLIENT)

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
                docs.append(doc)
            try:
                issue_bulk_index_documents_request(
                    upload_id=upload_id,
                    datasource=constants.DATASOURCE_NAME,
                    documents=docs,
                    is_first_page=False,
                    is_last_page=False)
            except indexing_api.ApiException as e:
                print("Exception while bulk indexing documents: %s\n" % e.body)
                exit(1)

