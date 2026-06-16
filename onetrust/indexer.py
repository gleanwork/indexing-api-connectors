import gleantools as tools
import gleanConstants as Constants

# indexing
import glean_indexing_api_client as indexing_api

# datasources
from glean_indexing_api_client.api import datasources_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from glean_indexing_api_client.model.property_definition import PropertyDefinition
from glean_indexing_api_client.model.property_group import PropertyGroup
from glean_indexing_api_client.model.custom_property import CustomProperty

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

# Concurrency
from concurrent.futures import ThreadPoolExecutor, as_completed


CONST = Constants.Constants()
customerApiEndpoint = "https://" + CONST.GLEAN_INSTANCE + "-be.glean.com/api/index/v1"

# Configure host and Bearer authorization: BearerAuth
configuration = indexing_api.Configuration(
    host = customerApiEndpoint,
    access_token = CONST.GLEAN_PUSH_API_TOKEN,
)
api_client = indexing_api.ApiClient(configuration)
datasources_api_client = datasources_api.DatasourcesApi(api_client)
documents_api_client = documents_api.DocumentsApi(api_client)
permissions_api_client = permissions_api.PermissionsApi(api_client)
#interact = Interact.Interact()

if CONST.getDebug() or CONST.isVerbose():
    print("Debug is:", CONST.getDebug())
    print("Verbose is:", CONST.isVerbose())
    print("Using the Glean API:", customerApiEndpoint)
    print("Glean API Token is:", CONST.GLEAN_PUSH_API_TOKEN)


def configureDatasource():
        lowRiskProp = PropertyDefinition(
            name = "lowriskcount",
            display_label = "LowRisk",
            property_type = "INT",
            # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
            #ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
            # object_property_options = ObjectPropertyOptions(
            #                               subobject_properties = PropertyDefinition()
            #                           )
            # group =
        )

        mediumRiskProp = PropertyDefinition(
            name = "mediumriskcount",
            display_label = "MediumRisk",
            property_type = "INT",
            # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
            #ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
            #ui_facet_order = 2,
            # object_property_options = ObjectPropertyOptions(
            #                               subobject_properties = PropertyDefinition()
            #                           )
            # group =
        )

        highRiskProp = PropertyDefinition(
            name = "highriskcount",
            display_label = "HighRisk",
            property_type = "INT",
            # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
            # ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
            #ui_facet_order = 2,
            # object_property_options = ObjectPropertyOptions(
            #                               subobject_properties = PropertyDefinition()
            #                           )
            # group =
        )

        veryHighRiskProp = PropertyDefinition(
            name = "veryhighriskcount",
            display_label = "VeryHighRisk",
            property_type = "INT",
            # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
            # ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
            #ui_facet_order = 2,
            # object_property_options = ObjectPropertyOptions(
            #                               subobject_properties = PropertyDefinition()
            #                           )
            # group =
        )

        residualRiskScoreProp = PropertyDefinition(
            name = "residualriskscore",
            display_label = "residualRiskScore",
            property_type = "INT",
            # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
            # ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
            ui_options = "SEARCH_RESULT",
            hide_ui_facet = False
            #ui_facet_order = 2,
            # object_property_options = ObjectPropertyOptions(
            #                               subobject_properties = PropertyDefinition()
            #                           )
            # group =
        )

        statusProp = PropertyDefinition(
            name = "assessmentstatus",
            display_label = "Assessment Status",
            property_type = "TEXT",
            ui_options = "SEARCH_RESULT",
            # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
            # ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
            hide_ui_facet = False
            #ui_facet_order = 2,
            # object_property_options = ObjectPropertyOptions(
            #                               subobject_properties = PropertyDefinition()
            #                           )
            # group =
        )


        Obj = ObjectDefinition(
            # doc_category = "UNCATEGORIZED" | "TICKETS" | "CRM" | "PUBLISHED_CONTENT" | "COLLABORATIVE_CONTENT" |
            #                "QUESTION_ANSWER" | "MESSAGING" | "CODE_REPOSITORY" | "CHANGE_MANAGEMENT" | "PEOPLE" |
            #                "EMAIL" | "SSO" | "ATS" | "KNOWLEDGE_HUB" | "EXTERNAL_SHORTCUT",
            property_definitions = [ lowRiskProp, mediumRiskProp, highRiskProp, veryHighRiskProp, residualRiskScoreProp, statusProp ]
        )
        datasource_config = CustomDatasourceConfig(
        datasource_category = CONST.DATASOURCE_CATEGORY,
        display_name = CONST.DATASOURCE_DISPLAYNAME,
        home_url = CONST.DATASOURCE_HOMEURL,
        icon_url = CONST.DATASOURCE_ICON,
        object_definitions = [Obj],
        is_entity_datasource = False,
        is_test_datasource = False,
        # Permissions will be specified by email addresses instead of a datasource-specific ID.
        is_user_referenced_by_email=False,
        name = CONST.DATASOURCE_NAME,
        #render_config_preset = "jira",
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
    return DocumentDefinition(
        id = doc['number'],
        title = doc["name"],
        #objectType = tools.cleanString(doc['objectType']),
        datasource = doc['datasource'],
        viewURL = doc['viewURL'],
        summary = ContentDefinition(**doc['summary']),
        body = ContentDefinition(**doc['body']),
        author = UserReferenceDefinition(**doc['creator']),
        permissions = DocumentPermissionsDefinition(**doc['permissions']),
        #createdAt = doc['createdAt'],
        updatedAt = doc['updatedAt'],
        #updatedBy = UserReferenceDefinition(**doc['updatedBy']),
        #tags = doc['tags'],
        #container = doc['container']
        custom_properties = [
                CustomProperty(
                    name="lowriskcount",
                    value=doc['lowRiskCount']
                ),
                CustomProperty(
                    name="mediumriskcount",
                    value=doc['mediumRiskCount']
                ),
                CustomProperty(
                    name="highriskcount",
                    value=doc['highRiskCount']
                ),
                CustomProperty(
                    name="veryhighriskcount",
                    value=doc['veryHighRiskCount']
                ),
                CustomProperty(
                    name="residualriskscore",
                    value=doc['residualRiskScore']
                ),
                CustomProperty(
                    name="assessmentstatus",
                    value=doc['status']
                )
            ]

        )


def indexDocs(filename):
    allAssessment = tools.csv_to_dict(filename)
    counter = 1
    for assessment in allAssessment:
        assessment['summary'] = tools.convert_str_map(assessment["summary"])
        assessment['body'] = tools.convert_str_map(assessment['body'])
        assessment['creator'] = tools.convert_str_map(assessment['creator'])
        assessment['permissions'] = tools.convert_str_map(assessment['permissions'])
        assessment['updatedAt'] = tools.convert_str_map(assessment['updatedAt'])
        #assessment['objectType'] = assessment['objectType'].replace(" ","")
        
        print("Importing Doc ID= " + str(assessment['assessmentId']) + " with the title= " + str(assessment['name']))

        document = getDocDef(assessment)

        request = IndexDocumentRequest(document)
        counter = counter + 1

        if CONST.getDebug() or CONST.isVerbose():
            print("\n--== user request ==--\n", request)
            print("\n")

        if not CONST.getDebug():
            try:
                documents_api_client.indexdocument_post(request)
            except indexing_api.ApiException as e:
                print("Exception when calling DocumentsApi->indexdocument_post: %s\n" % e)


# def bulkIndexPeople(filename,uploadId):
#     people = tools.csv_to_dict(filename)
#     peopleCounter = 0
#     allUsers = []
#     isActive = True

#     for person in people:
#         peopleCounter += 1
#         if person['email'] != "":

#             #isActive = person.is_suspended == False

#             user = DatasourceUserDefinition(
#                 name = person['name'],
#                 email = person['email'],
#                 isActive = isActive,
#                 userId = person['userId']
#             )

#             if CONST.getDebug() or CONST.isVerbose():
#                 print(user)

#             allUsers.append(user)
#         else:
#             print(f"Skipping user: {person['userId']}")
#             errorList.append(person['userId'])
#             continue

#     bulkUserRequest = BulkIndexUsersRequest(
#         upload_id = uploadId,
#         datasource = CONST.DATASOURCE_NAME,
#         users = allUsers,
#         is_first_page = True,
#         is_last_page = True,
#         force_restart_upload = True
#     )

#     if CONST.getDebug() or CONST.isVerbose():
#         print("\n--== user request ==--\n", bulkUserRequest)
#         print("\n")

#     if not CONST.getDebug():
#         try:
#             if CONST.isVerbose():
#                 print("making permissions API call.\n")
#             permissions_api_client.bulkindexusers_post(bulkUserRequest)
#         except indexing_api.ApiException as e:
#             print("Exception when calling PermissionApi->indexuser_post: %s\n" % e)


# def bulkIndexBatch(uploadId,batch,firstPage,lastPage):
#     print("firstPage=" + str(firstPage) + " lastPage="+ str(lastPage))
#     bulkRequest = BulkIndexDocumentsRequest(
#         upload_id = uploadId,
#         datasource = CONST.DATASOURCE_NAME,
#         documents = batch,
#         is_first_page = bool(firstPage),
#         is_last_page = bool(lastPage),
#         force_restart_upload = bool(firstPage),
#         disable_stale_document_deletion_check = bool(lastPage)
#         )

#     if CONST.getDebug() or CONST.isVerbose(): 
#         print("\n--== user request ==--\n", bulkRequest)
#         print("\n")
    
#     if not CONST.getDebug():
#         try:
#             documents_api_client.bulkindexdocuments_post(bulkRequest)
#             print("Batch Index is completed")
#         except indexing_api.ApiException as e:
#             print("Exception when calling DocumentsApi->bulkindexdocuments_post: %s\n" % e)
#             if CONST.isVerbose():
#                 print("Failed batch docs\n")
#                 print(bulkRequest)


# def bulkIndexDocs(filename,uploadId, firstPage,lastPage,counter):
#     allPages = tools.csv_to_dict(filename)
#     batch = []
#     totalPages = len(allPages)
#     print("Bulk indexing docs count = ", totalPages)

#     NUM_THREADS = 10

#     # First page: 0 documents
#     bulkIndexBatch(uploadId, [], firstPage=firstPage, lastPage=False)

#     with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
#         futures = []
#         for page in allPages:
#             page['summary'] = tools.convert_str_map(page['summary'])
#             if not str(page['body']) == "{'mime_type': 'text/html', 'textContent': None}":
#                 page['body'] = tools.convert_str_map(page['body'])
#             else:
#                 page['body'] = page['summary']
#             page['author'] = tools.convert_str_map(page['author'])
#             page['permissions'] = tools.convert_str_map(page['permissions'])
#             page['updatedBy'] = tools.convert_str_map(page['updatedBy'])
#             page['objectType'] = page['objectType'].replace(" ","")
#             doc = getDocDef(page)
            
#             # create batch for the bulkk index
#             batch.append(doc)
#             counter += 1
#             if (counter%int(CONST.BULK_BATCH_SIZE)) == 0:
#                 futures.append(executor.submit(bulkIndexBatch,uploadId,batch.copy(),firstPage=False,lastPage=False))
#                 batch.clear()

#     # Wait for futures to complete, throw exception if any batch fails and avoid sending last page if failed
#     for future in as_completed(futures):
#         future.result()            

#     # Last page = Either empty or has less than `batch size` documents
#     bulkIndexBatch(uploadId,batch.copy(),firstPage=False,lastPage=lastPage)
    
