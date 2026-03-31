import json
import logging
import os
import sys
import time

import requests
import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.api import datasources_api, documents_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.document_permissions_definition import DocumentPermissionsDefinition

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PAGE_SIZE = 10


def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)


def build_glean_client(glean_domain, glean_api_token):
    configuration = indexing_api.Configuration(
        host=f"https://{glean_domain}-be.glean.com/api/index/v1",
        access_token=glean_api_token,
    )
    return indexing_api.ApiClient(configuration)


def add_datasource(api_client, config):
    datasource_config = CustomDatasourceConfig(
        name=config["datasource_name"],
        display_name=config["datasource_display_name"],
        datasource_category=config["datasource_category"],
        url_regex=config["url_regex"],
        is_test_datasource=config.get("is_test_datasource", True),
        object_definitions=[
            ObjectDefinition(
                doc_category=config["datasource_category"],
                name=config["object_type"],
            )
        ],
    )
    try:
        api = datasources_api.DatasourcesApi(api_client)
        api.adddatasource_post(datasource_config)
        logging.info("Datasource '%s' created/updated.", config["datasource_name"])
    except indexing_api.ApiException as e:
        logging.error("Failed to add datasource: %s", e.body)
        sys.exit(1)


def fetch_data(config, source_api_token):
    source = config["source_api"]
    headers = {"Authorization": f"Bearer {source_api_token}"}
    headers.update(source.get("headers", {}))

    method = source.get("method", "GET").upper()
    if method == "GET":
        resp = requests.get(source["endpoint"], headers=headers, params=source.get("params", {}))
    else:
        resp = requests.post(source["endpoint"], headers=headers, json=source.get("params", {}))

    resp.raise_for_status()
    data = resp.json()

    results_key = source.get("results_key")
    if results_key:
        data = data[results_key]

    logging.info("Fetched %d items from source API.", len(data))
    return data


def build_document(item, config):
    mappings = config["field_mappings"]
    datasource = config["datasource_name"]
    object_type = config["object_type"]

    doc_id = str(item[mappings["id"]])
    title = str(item[mappings["title"]])
    body = str(item.get(mappings.get("body", ""), ""))
    view_url = str(item[mappings["view_url"]])
    mime_type = mappings.get("mime_type", "text/html")

    return DocumentDefinition(
        datasource=datasource,
        object_type=object_type,
        id=doc_id,
        title=title,
        view_url=view_url,
        body=ContentDefinition(mime_type=mime_type, text_content=body),
        permissions=DocumentPermissionsDefinition(allow_anonymous_access=True),
    )


def bulk_index(api_client, config, items):
    upload_id = f"upload-{config['datasource_name']}-{int(time.time())}"
    doc_api = documents_api.DocumentsApi(api_client)
    datasource = config["datasource_name"]

    documents = [build_document(item, config) for item in items]

    # Send first page marker
    doc_api.bulkindexdocuments_post(
        BulkIndexDocumentsRequest(
            upload_id=upload_id,
            datasource=datasource,
            documents=[],
            is_first_page=True,
            is_last_page=False,
            force_restart_upload=False,
        )
    )

    # Send documents in pages
    for i in range(0, len(documents), PAGE_SIZE):
        batch = documents[i : i + PAGE_SIZE]
        doc_api.bulkindexdocuments_post(
            BulkIndexDocumentsRequest(
                upload_id=upload_id,
                datasource=datasource,
                documents=batch,
                is_first_page=False,
                is_last_page=False,
                force_restart_upload=False,
            )
        )
        logging.info("Indexed batch %d-%d of %d.", i + 1, i + len(batch), len(documents))

    # Send last page marker
    doc_api.bulkindexdocuments_post(
        BulkIndexDocumentsRequest(
            upload_id=upload_id,
            datasource=datasource,
            documents=[],
            is_first_page=False,
            is_last_page=True,
            force_restart_upload=False,
        )
    )

    logging.info("Bulk indexing complete. Upload ID: %s", upload_id)


def main():
    # Required env vars
    glean_domain = os.environ.get("GLEAN_DOMAIN")
    glean_api_token = os.environ.get("GLEAN_API_TOKEN")
    source_api_token = os.environ.get("SOURCE_API_TOKEN")

    if not all([glean_domain, glean_api_token, source_api_token]):
        logging.error("Missing required env vars: GLEAN_DOMAIN, GLEAN_API_TOKEN, SOURCE_API_TOKEN")
        sys.exit(1)

    config_path = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.json"))
    config = load_config(config_path)

    api_client = build_glean_client(glean_domain, glean_api_token)

    add_datasource(api_client, config)
    items = fetch_data(config, source_api_token)
    bulk_index(api_client, config, items)

    logging.info("Done.")


if __name__ == "__main__":
    main()
