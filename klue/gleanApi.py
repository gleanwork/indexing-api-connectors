# pylint: disable-all  
import copy, time, json 
import gleanConstants as Constants 
 
# Main Glean SDK 
import glean_indexing_api_client as indexing_api 
 
# datasources 
from glean_indexing_api_client.api import datasources_api 
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig 
from glean_indexing_api_client.model.object_definition import ObjectDefinition 
from glean_indexing_api_client.model.property_definition import PropertyDefinition 
 
# permissions  
from glean_indexing_api_client.api import permissions_api 
from glean_indexing_api_client.model.index_user_request import IndexUserRequest  
from glean_indexing_api_client.model.datasource_user_definition import (  
    DatasourceUserDefinition,  
)  
 
# Documents  
from glean_indexing_api_client.api import documents_api 
from glean_indexing_api_client.model.index_document_request import IndexDocumentRequest  
from glean_indexing_api_client.model.document_definition import DocumentDefinition  
from glean_indexing_api_client.model.content_definition import ContentDefinition  
from glean_indexing_api_client.model.object_definition import ObjectDefinition  
from glean_indexing_api_client.model.property_definition import PropertyDefinition  
from glean_indexing_api_client.model.property_group import PropertyGroup  
from glean_indexing_api_client.model.bulk_index_users_request import BulkIndexUsersRequest  
from glean_indexing_api_client.model.bulk_index_groups_request import BulkIndexGroupsRequest  
from glean_indexing_api_client.model.bulk_index_memberships_request import BulkIndexMembershipsRequest  
from glean_indexing_api_client.model.datasource_bulk_membership_definition import DatasourceBulkMembershipDefinition  
from glean_indexing_api_client.model.datasource_group_definition import DatasourceGroupDefinition  
from glean_indexing_api_client.model.custom_property import CustomProperty  
from glean_indexing_api_client.model.object_property_options import ObjectPropertyOptions  
from glean_indexing_api_client.model.user_reference_definition import (  
    UserReferenceDefinition,  
)  
 
# Documents
from glean_indexing_api_client.model.index_document_request import IndexDocumentRequest
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
from glean_indexing_api_client.model.process_all_documents_request import ProcessAllDocumentsRequest

# Concurrency
from concurrent.futures import ThreadPoolExecutor, as_completed

from glean_indexing_api_client.model.document_permissions_definition import (  
    DocumentPermissionsDefinition,  
)  
 
 
# 
# env vars 
# 
CONST = Constants.Constants() 
 
#  
# Configure host and Bearer authorization: BearerAuth 
# 
configuration = indexing_api.Configuration( 
  host = CONST.GLEAN_INDEX_API, access_token = CONST.GLEAN_PUSH_API_TOKEN 
) 
 
api_client = indexing_api.ApiClient(configuration) 
datasources_api_client = datasources_api.DatasourcesApi(api_client) 
documents_api_client = documents_api.DocumentsApi(api_client) 
 
 
def configureDatasource():  
    """Configuring a datasource"""  
    projectProp = PropertyDefinition(  
        name = "competitor",  
        display_label = "Competitor",  
        display_label_plural = "Competitors",  
        property_type = "TEXT",  
        ui_options = "SEARCH_RESULT",  
        hide_ui_facet = False,  
        ui_facet_order = 1,  
    )  
 
    entryObj = ObjectDefinition(  
        name = "battlecard",  
        display_label = "Battlecard",  
        property_definitions = [ projectProp ],  
    )  
 
    datasource_config = CustomDatasourceConfig(  
        datasource_category = CONST.DATASOURCE_CATEGORY,  
        display_name = CONST.DATASOURCE_DISPLAYNAME,  
        home_url = CONST.DATASOURCE_HOMEURL,  
        icon_url = CONST.DATASOURCE_ICON,  
        is_entity_datasource = False,  
        is_test_datasource = False,  
        is_user_referenced_by_email = True,  
        name = CONST.DATASOURCE_NAME,  
        object_definitions = [ entryObj ],  
        suggestion_text = "Klue Information Cards",  
        trust_url_regex_for_view_activity = False,  
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




#
#  i n d e x D o c
#
#  perform checks on data and index document
def indexDoc(docObj):

    # Building request from document object
    document = DocumentDefinition(**docObj)
    request = IndexDocumentRequest(document)

    if CONST.getDebug() or CONST.isVerbose():
        printable = copy.deepcopy(document)
        try:
            printable['body']['binary_content'] = "bytes: " + str(len(document['body']['binary_content']))
        except:
            printable['body']['text_content'] = "text: " + str(len(document['body']['text_content']))
        
        print("document request: ", printable)
        print("\n\n")

    if not CONST.getDebug():
        try:
            print(f"Indexing Document: {docObj['view_url']}")
            documents_api_client.indexdocument_post(request)
        except indexing_api.ApiException as e:
            print("Exception when calling documents_client->indexdocument_post: %s\n" % e)



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
        print("\n--== batch request ==--\n", bulkRequest)
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



def bulkIndexDocs(uploadId, documents):
    counter = 0
    batch = []
    totalDocuments = len(documents)
    print("Bulk indexing docs count = ", totalDocuments)

    NUM_THREADS = 1

    # First page: 0 documents
    bulkIndexBatch(uploadId, [], firstPage=True, lastPage=False)

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for document in documents:
            doc = DocumentDefinition(**document)
            
            # create batch for the bulkk index
            batch.append(doc)
            counter += 1
            if (counter % int(CONST.BATCH_SIZE)) == 0:
                futures.append(executor.submit(bulkIndexBatch,uploadId,batch.copy(),firstPage=False,lastPage=False))
                batch.clear()

    # Wait for futures to complete, throw exception if any batch fails and avoid sending last page if failed
    for future in as_completed(futures):
        future.result()            

    # Last page = Either empty or has less than `batch size` documents
    bulkIndexBatch(uploadId,batch.copy(),firstPage=False,lastPage=True)
   