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

PAGE_SIZE = 10
DATASOURCE = "databricks"

# Configure host and Bearer authorization
configuration = indexing_api.Configuration(
    host="https://<YOUR_GLEAN_DOMAIN>-be.glean.com/api/index/v1",
    access_token="<YOUR_GLEAN_API_KEY>")

# Initialize API client
api_client = indexing_api.ApiClient(configuration)


example_list_dashboards = json.loads("""{
    "count": 1,
    "page": 1,
    "page_size": 25,
    "results": [
        {
            "id": "968b21bc-5da3-439c-a261-0fd72f00e4cc",
            "slug": "example-v1-dashboard",
            "name": "Example v1 dashboard",
            "user_id": 8123713393700284,
            "dashboard_filters_enabled": false,
            "widgets": null,
            "options": {
                "parent": "folders/978236875178135",
                "folder_node_status": "ACTIVE",
                "folder_node_internal_name": "tree/978236875178153"
            },
            "is_draft": false,
            "tags": [],
            "updated_at": "2023-11-15T20:05:17Z",
            "created_at": "2023-11-15T20:04:46Z",
            "version": 3,
            "color_palette": null,
            "run_as_role": null,
            "run_as_service_principal_id": null,
            "data_source_id": null,
            "warehouse_id": null,
            "user": {
                "id": 8123713393700284,
                "name": "alexis.deschamps@databricks.com",
                "email": "alexis.deschamps@databricks.com"
            },
            "is_favorite": false
        }
    ]
}""")

example_dashboard = json.loads("""{
    "id": "968b21bc-5da3-439c-a261-0fd72f00e4cc",
    "slug": "example-v1-dashboard",
    "name": "Example v1 dashboard",
    "user_id": 8123713393700284,
    "dashboard_filters_enabled": false,
    "widgets": [],
    "options": {
        "parent": "folders/978236875178135",
        "folder_node_status": "ACTIVE",
        "folder_node_internal_name": "tree/978236875178153"
    },
    "is_draft": false,
    "tags": [],
    "updated_at": "2023-11-15T20:05:17Z",
    "created_at": "2023-11-15T20:04:46Z",
    "version": 3,
    "color_palette": null,
    "run_as_role": null,
    "run_as_service_principal_id": null,
    "data_source_id": null,
    "warehouse_id": null,
    "is_favorite": false,
    "user": {
        "id": 8123713393700284,
        "name": "alexis.deschamps@databricks.com",
        "email": "alexis.deschamps@databricks.com"
    },
    "parent": "folders/978236875178135",
    "is_archived": false,
    "can_edit": true,
    "permission_tier": "CAN_MANAGE"
}""")



def fetch_all_wikipedia_articles(search_term: str = 'pizza', limit: int = 50):
    """Fetch Wikipedia articles related to a search term"""
    wikipedia_api_url = f"https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&srsearch={search_term}%20&utf8=1&srlimit={limit}"
    response = requests.get(wikipedia_api_url)
    return response.json()["query"]["search"]


def get_document_definition(article):
    """Construct document definition from Wikipedia article"""
    title = article["title"]
    url_suffix = title.replace(" ", "_")  # URL is derived from the title
    page_url = f"https://en.wikipedia.org/wiki/{url_suffix}"
    page_snippet = article["snippet"]
    page_id = str(article["pageid"])
    return DocumentDefinition(
        datasource=DATASOURCE,
        object_type="Article",
        id=page_id,
        title=title,
        view_url=page_url,  # should match the url_regex in the datasource config
        body=ContentDefinition(
            mime_type="text/html",
            text_content=page_snippet),
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


def bulk_index_documents_sequential(upload_id, articles, page_size=10):
    """Bulk index documents sequentially"""
    documents = []

    issue_bulk_index_documents_request(
        upload_id=upload_id,
        datasource=DATASOURCE,
        documents=[],
        is_first_page=True,
        is_last_page=False)

    for (i, article) in enumerate(articles):
        documents.append(get_document_definition(article))
        if (i + 1) % page_size == 0:
            issue_bulk_index_documents_request(
                upload_id=upload_id,
                datasource=DATASOURCE,
                documents=documents.copy(),
                is_first_page=False,
                is_last_page=False)
            documents.clear()

    issue_bulk_index_documents_request(
        upload_id=upload_id,
        datasource=DATASOURCE,
        documents=[],
        is_first_page=False,
        is_last_page=True)


def bulk_index_documents_concurrent(upload_id, articles, page_size=10):
    """Bulk index documents concurrently, allowing for intermediate pages being uploaded parallely"""
    documents = []
    NUM_THREADS = 5

    issue_bulk_index_documents_request(
        upload_id=upload_id,
        datasource=DATASOURCE,
        documents=[],
        is_first_page=True,
        is_last_page=False)

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for (i, article) in enumerate(articles):
            documents.append(get_document_definition(article))
            if (i + 1) % page_size == 0:
                futures.append(
                    executor.submit(
                        issue_bulk_index_documents_request,
                        upload_id,
                        DATASOURCE,
                        documents.copy(),
                        False,
                        False))
                documents.clear()
        for future in as_completed(futures):
            future.result()

    issue_bulk_index_documents_request(
        upload_id=upload_id,
        datasource=DATASOURCE,
        documents=[],
        is_first_page=False,
        is_last_page=True)


def main():
    add_datasource()
    articles = fetch_all_wikipedia_articles('pizza', 50)
    upload_id = f'upload-wikipedia-documents-{time.time()}'
    try:
        bulk_index_documents_sequential(upload_id, articles)

        # Alternatively, bulk index documents concurrently
        # bulk_index_documents_concurrent(upload_id, articles)

        print("Bulk indexing completed successfully.")

    except indexing_api.ApiException as e:
        print("Exception while bulk indexing documents: %s\n" % e.body)
        exit(1)


if __name__ == "__main__":
    main()
