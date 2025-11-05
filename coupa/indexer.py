# pylint: disable-all
import gleantools as tools
import gleanConstants as Constants
import sys

# indexing
import glean_indexing_api_client as indexing_api

# datasources
from glean_indexing_api_client.api import datasources_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from glean_indexing_api_client.model.property_definition import PropertyDefinition
from glean_indexing_api_client.model.property_group import PropertyGroup

# permissions
from glean_indexing_api_client.api import permissions_api
from glean_indexing_api_client.model.index_user_request import IndexUserRequest
from glean_indexing_api_client.model.datasource_user_definition import (
    DatasourceUserDefinition,
)
from glean_indexing_api_client.model.bulk_index_memberships_request import BulkIndexMembershipsRequest
from glean_indexing_api_client.model.bulk_index_users_request import BulkIndexUsersRequest
from glean_indexing_api_client.model.bulk_index_groups_request import BulkIndexGroupsRequest
from glean_indexing_api_client.model.datasource_bulk_membership_definition import DatasourceBulkMembershipDefinition
from glean_indexing_api_client.model.datasource_group_definition import DatasourceGroupDefinition
from glean_indexing_api_client.model.object_property_options import ObjectPropertyOptions
from glean_indexing_api_client.model.user_reference_definition import (
    UserReferenceDefinition,
)
from glean_indexing_api_client.model.document_permissions_definition import (
    DocumentPermissionsDefinition,
)

# Documents
from glean_indexing_api_client.api import documents_api
from glean_indexing_api_client.model.index_document_request import IndexDocumentRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.process_all_documents_request import ProcessAllDocumentsRequest
from glean_indexing_api_client.model.custom_property import CustomProperty

# Concurrency
from concurrent.futures import ThreadPoolExecutor, as_completed


CONST = Constants.Constants()
customerApiEndpoint = "https://" + CONST.GLEAN_INSTANCE + "-be.glean.com/api/index/v1"
errorList = []

# Configure host and Bearer authorization: BearerAuth
configuration = indexing_api.Configuration(
    host = customerApiEndpoint,
    access_token = CONST.GLEAN_PUSH_API_TOKEN,
)
api_client = indexing_api.ApiClient(configuration)
datasources_api_client = datasources_api.DatasourcesApi(api_client)
documents_api_client = documents_api.DocumentsApi(api_client)
permissions_api_client = permissions_api.PermissionsApi(api_client)

if CONST.getDebug() or CONST.isVerbose():
    print("Debug is:", CONST.getDebug())
    print("Verbose is:", CONST.isVerbose())
    print("MIN_IDX:", CONST.MIN_IDX)
    print("MAX_IDX:", CONST.MAX_IDX)
    print("DELAY:", CONST.DELAY)
    print("CRAWL_TYPE:", CONST.CRAWL_TYPE)
    print("MAX_THREADS:", CONST.MAX_THREADS)
    print("DATASOURCE_USERDOMAINS:", CONST.DATASOURCE_USERDOMAINS)
    print("Using the Glean API:", customerApiEndpoint)
    print("Glean API Token is:", CONST.GLEAN_PUSH_API_TOKEN)


def configureDatasource():
        supplierCountry = PropertyDefinition (
            name = "suppliercountry",
            display_label = "Country",
            property_type = "TEXT",
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
        )

        supplierState = PropertyDefinition (
            name = "supplierstate",
            display_label = "State",
            property_type = "TEXT",
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
        )

        supplierCity = PropertyDefinition (
            name = "suppliercity",
            display_label = "City",
            property_type = "TEXT",
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
        )

        supplierObj = ObjectDefinition (
            name = "supplier",
            display_label = "supplier",
            property_definitions = [supplierState, supplierCity, supplierCountry]
        )

        datasource_config = CustomDatasourceConfig(
        datasource_category = CONST.DATASOURCE_CATEGORY,
        display_name = CONST.DATASOURCE_DISPLAYNAME,
        home_url = CONST.DATASOURCE_HOMEURL,
        icon_url = CONST.DATASOURCE_ICON,
        is_entity_datasource = False,
        is_test_datasource = False,
        # Permissions will be specified by email addresses instead of a datasource-specific ID.
        is_user_referenced_by_email=False,
        name = CONST.DATASOURCE_NAME,
        object_definitions = [supplierObj],
        render_config_preset = "gdrive",
        suggestion_text = "Search for Support",
        trust_url_regex_for_view_activity = True,
        url_regex = CONST.DATASOURCE_URLREGEX
        )
        
        if CONST.getDebug() or CONST.isVerbose():
            print("\n--== datasource configuration ==--\n", datasource_config)
            print("\n\n")
    
        if not CONST.getDebug():
            try:
                if CONST.isVerbose():
                    print("making datasources API call.\n")
                datasources_api_client.adddatasource_post(datasource_config)
            except indexing_api.ApiException as e:
                print("Exception when calling DatasourcesApi->adddatasource_post: %s\n" % e)


def getDocDef(doc):

    if 'custom' in doc:
        return DocumentDefinition(
            id = doc['id'],
            title = doc['title'],
            objectType = doc['objectType'],
            datasource = doc['datasource'],
            viewURL = doc['viewURL'],
            status = doc['status'],
            summary = ContentDefinition(**doc['summary']),
            body = ContentDefinition(**doc['body']),
            author = UserReferenceDefinition(**doc['author']),
            permissions = DocumentPermissionsDefinition(**doc['permissions']),
            createdAt = doc['createdAt'],
            updatedAt = doc['updatedAt'],
            #updatedBy = UserReferenceDefinition(**doc['updatedBy']),
            custom_properties = doc['custom']
        )

    else:
        return DocumentDefinition(
            id = doc['id'],
            title = doc['title'],
            objectType = doc['objectType'],
            datasource = doc['datasource'],
            viewURL = doc['viewURL'],
            status = doc['status'],
            summary = ContentDefinition(**doc['summary']),
            body = ContentDefinition(**doc['body']),
            author = UserReferenceDefinition(**doc['author']),
            permissions = DocumentPermissionsDefinition(**doc['permissions']),
            createdAt = doc['createdAt'],
            updatedAt = doc['updatedAt'],
            #updatedBy = UserReferenceDefinition(**doc['updatedBy']),
        )



def indexDocs(doc):

    print("Importing Doc ID= " + str(doc['id']) + " with the title= " + str(doc['title']))

    document = getDocDef(doc)

    request = IndexDocumentRequest(document)

    if CONST.getDebug() or CONST.isVerbose():
        print("\n--== user request ==--\n", request)
        print("\n")

    if not CONST.getDebug():
        try:
            documents_api_client.indexdocument_post(request)
        except indexing_api.ApiException as e:
            print("Exception when calling DocumentsApi->indexdocument_post: %s\n" % e)


def bulkIndexBatch(uploadId,batch,firstPage,lastPage):
    print("firstPage=" + str(firstPage) + " lastPage="+ str(lastPage))
    bulkRequest = BulkIndexDocumentsRequest(
        upload_id = uploadId,
        datasource = CONST.DATASOURCE_NAME,
        documents = batch,
        is_first_page = bool(firstPage),
        is_last_page = bool(lastPage),
        force_restart_upload = bool(firstPage),
        disable_stale_document_deletion_check = bool(lastPage)
        )

    if CONST.getDebug() or CONST.isVerbose(): 
        print("\n--== user request ==--\n", bulkRequest)
        print("\n")
    
    if not CONST.getDebug():
        try:
            documents_api_client.bulkindexdocuments_post(bulkRequest)
            print("Batch Index is completed")
        except indexing_api.ApiException as e:
            print("Exception when calling DocumentsApi->bulkindexdocuments_post: %s\n" % e)
            if CONST.isVerbose():
                print("Failed batch docs\n")
                print(bulkRequest)


def bulkIndexDocs(filename,uploadId, firstPage,lastPage,counter):
    allPages = tools.csv_to_dict(filename)
    batch = []
    totalPages = len(allPages)
    print("Bulk indexing docs count = ", totalPages)

    NUM_THREADS = 10

    # First page: 0 documents
    bulkIndexBatch(uploadId, [], firstPage=firstPage, lastPage=False)

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for page in allPages:
            page['summary'] = tools.convert_str_map(page['summary'])
            if not str(page['body']) == "{'mime_type': 'text/html', 'textContent': None}":
                page['body'] = tools.convert_str_map(page['body'])
            else:
                page['body'] = page['summary']
            page['author'] = tools.convert_str_map(page['author'])
            page['permissions'] = tools.convert_str_map(page['permissions'])
            page['updatedBy'] = tools.convert_str_map(page['updatedBy'])
            page['objectType'] = page['objectType'].replace(" ","")
            doc = getDocDef(page)
            
            # create batch for the bulkk index
            batch.append(doc)
            counter += 1
            if (counter%int(CONST.BULK_BATCH_SIZE)) == 0:
                futures.append(executor.submit(bulkIndexBatch,uploadId,batch.copy(),firstPage=False,lastPage=False))
                batch.clear()

    # Wait for futures to complete, throw exception if any batch fails and avoid sending last page if failed
    for future in as_completed(futures):
        future.result()            

    # Last page = Either empty or has less than `batch size` documents
    bulkIndexBatch(uploadId,batch.copy(),firstPage=False,lastPage=lastPage)
