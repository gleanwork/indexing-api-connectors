#!python3
"""Glean Benchling Crawler

Thank you for using the Glean Benchling Crawler example

This script will help you crawl a set of URLs from a csv and import them
into a datasource.


---------------------------
| Hit any key to continue |
---------------------------

"""

#
# Imports
#
import os, json, time, copy
import gleantools as tools
import gleanConstants as Constants
import benchling as Benchling

# indexing
import glean_indexing_api_client as indexing_api

# datasources
from glean_indexing_api_client.api import datasources_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig

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
from glean_indexing_api_client.model.document_permissions_definition import (
    DocumentPermissionsDefinition,
)


# Environment fix for help and then display current help
os.environ['PAGER'] = 'more'
#help(__name__)



#
# Initializaiton
#
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
benchling = Benchling.Benchling()

if CONST.getDebug() or CONST.isVerbose():
    print("Debug is:", CONST.getDebug())
    print("Verbose is:", CONST.isVerbose())
    print("MIN_IDX:", CONST.MIN_IDX)
    print("MAX_IDX:", CONST.MAX_IDX)
    print("DELAY:", CONST.DELAY)
    print("CRAWL_TYPE:", CONST.CRAWL_TYPE)
    print("MAX_THREADS:", CONST.MAX_THREADS)
    print("Using the Glean API:", customerApiEndpoint)
    print("Glean API Token is:", CONST.GLEAN_PUSH_API_TOKEN)

#
# configureDatasource
#
def configureDatasource():
    """Configuring a datasource"""

    projectProp = PropertyDefinition(
        name = "benchProject",
        display_label = "Project",
        display_label_plural = "Projects",
        # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
        property_type = "TEXT",
        # ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
        ui_options = "SEARCH_RESULT",
        hide_ui_facet = False,
        ui_facet_order = 1,
        # object_property_options = ObjectPropertyOptions(
        #                               subobject_properties = PropertyDefinition()
        #                           )
        # group =
    )

    folderProp = PropertyDefinition(
        name = "benchFolder",
        display_label = "Folder",
        display_label_plural = "Folders",
        # property_type = Enum: "TEXT" "DATE" "INT" "USERID" "PICKLIST" "TEXTLIST"
        property_type = "TEXT",
        # ui_options = Enum: "NONE" "SEARCH_RESULT" "DOC_HOVERCARD"
        ui_options = "SEARCH_RESULT",
        hide_ui_facet = False,
        ui_facet_order = 2,
        # object_property_options = ObjectPropertyOptions(
        #                               subobject_properties = PropertyDefinition()
        #                           )
        # group =
    )



    entryObj = ObjectDefinition(
        name = "entry",
        display_label = "Entry",
        # doc_category = "UNCATEGORIZED" | "TICKETS" | "CRM" | "PUBLISHED_CONTENT" | "COLLABORATIVE_CONTENT" |
        #                "QUESTION_ANSWER" | "MESSAGING" | "CODE_REPOSITORY" | "CHANGE_MANAGEMENT" | "PEOPLE" |
        #                "EMAIL" | "SSO" | "ATS" | "KNOWLEDGE_HUB" | "EXTERNAL_SHORTCUT",
        property_definitions = [ projectProp, folderProp ],
        # property_groups = [ PropertyGroup (
        #     name = "details",
        #     display_label = "Details"
        # ) ]
    )

    datasource_config = CustomDatasourceConfig(
        # aliases = [ str, str ],
        # canonicalizing_title_regex = [ CanonicalizingRegexType() ],
        # canonicalizing_url_regex = [ CanonicalizingRegexType() ],
        # connector_type = ConnectorType(),
        # crawler_seed_urls = [ str ],
        datasource_category = CONST.DATASOURCE_CATEGORY,
        display_name = CONST.DATASOURCE_DISPLAYNAME,
        # hide_built_in_facets = [ "TYPE", "TAG", "AUTHOR", "OWNER" ],
        home_url = CONST.DATASOURCE_HOMEURL,
        icon_url = CONST.DATASOURCE_ICON,
        # identity_datasource_name = str,
        # include_utm_source = True,
        is_entity_datasource = False,
        # is_on_prem = False,
        is_test_datasource = False,
        # Permissions will be specified by email addresses instead of a datasource-specific ID.
        is_user_referenced_by_email=False,
        name = CONST.DATASOURCE_NAME,
        object_definitions = [ entryObj ],
        # product_access_group = str,
        # quicklinks = [ Quicklink ]
        # redlist_title_regex = str,
        suggestion_text = "Search for Support",
        trust_url_regex_for_view_activity = True,
        url_regex = CONST.DATASOURCE_URLREGEX,
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
# Index Docs
#
def indexDocs(docs):
    counter = 0

    folderList = benchling.getFolders()
    projectList = benchling.getProjects()

    print(f"retrieved {len(folderList)} folders from API")
    print(f"retrieved {len(projectList)} projects from API")

    def getProjectNameFromId(projectId):
        for project in projectList:
            # print ("project", project)
            if project.id == projectId:
                return project.name
        return projectId

    def getFolderNameFromId(folderId):
        for folder in folderList:
            # print ("folder", folder)
            if folder.id == folderId:
                return folder.name

    def resolveFolderId(folderId, prefix = None):
        # print(f"resolveFolderId - folderId: {folderId} - prefix: {prefix}")
        for folder in folderList:
            # print ("folder", folder)
            if folder.id == folderId:
                if folder.parent_folder_id == None:
                    if prefix == None:
                        return folder.name
                    else:
                        return folder.name + " / " + str(prefix)
                else:
                    if prefix == None:
                        return resolveFolderId(folder.parent_folder_id, folder.name)
                    else:
                        return resolveFolderId(folder.parent_folder_id, folder.name) + " / " + str(prefix)

    def getProjectFromFolder(folderId):
        for folder in folderList:
            if folder.id == folderId:
                return folder.project_id                 
        
        return ""


    for doc in docs:
        counter = counter + 1

        folderId = doc['container']
        projectId = getProjectFromFolder(folderId)

        if counter >= CONST.MIN_IDX and counter <= CONST.MAX_IDX and doc['id']:
            if counter > 1: time.sleep(CONST.DELAY)
            print(f"Importing Document: {counter} - title: {doc['title']} - id: {doc['id']}")

            allowedUserList = [ UserReferenceDefinition(datasource_user_id = doc['author']) ]
            if (doc['author'] != doc['updated_by'] != doc['author']):
                allowedUserList.append(UserReferenceDefinition(datasource_user_id = doc['updated_by']))

            docPermissions = DocumentPermissionsDefinition(
                allow_all_datasource_users_access = True,
                allowed_users = allowedUserList
            )

            if CONST.PUBLIC_ONLY == True:
                docPermissions['allow_anonymous_access'] = True


            # building document request JSON
            document = DocumentDefinition(
                #additional_urls =
                author = UserReferenceDefinition(datasource_user_id = doc['author']),
                body = ContentDefinition(**doc['body']),
                container = resolveFolderId(doc['container']),
                created_at = doc['created_at'],
                custom_properties = [ CustomProperty(name="benchProject", value=getProjectNameFromId(projectId)),
                                      CustomProperty(name="benchFolder", value=getFolderNameFromId(folderId)) ],
                datasource = CONST.DATASOURCE_NAME,
                id = doc['id'],
                #interactions =
                object_type = "entry",
                #owner =
                permissions = docPermissions,
                #status =
                #summary =
                #tags =
                title = doc['title'],
                updated_at = doc['updated_at'],
                updated_by = UserReferenceDefinition(datasource_user_id = doc['updated_by']),
                view_url = doc['view_url']
            )

            request = IndexDocumentRequest(document)

            if CONST.getDebug() or CONST.isVerbose():
                printable = copy.deepcopy(document)
                try:
                    printable['body']['binary_content'] = "bytes: " + str(len(printable['body']['binary_content']))
                except:
                    printable['body']['text_content'] = "text: " + str(len(printable['body']['text_content']))
                
                print("\n--== document definition ==--\n", printable)

                print("\n\n")

            if not CONST.getDebug():
                try:
                     if CONST.isVerbose():
                        print("making documents API call.\n")
                     documents_api_client.indexdocument_post(request)
                except indexing_api.ApiException as e:
                    print("Exception when calling DocumentsApi->indexdocument_post: %s\n" % e)



#
# Indexing the groups
#
def indexGroups(inputGroups):
    groupCounter = 0
    groups = []
    for group in inputGroups:
        groupCounter += 1

        groups.append(
            DatasourceGroupDefinition(
                name = tools.cleanString(group['groupname'])
            )
        )

    bulkRequest = BulkIndexGroupsRequest(
            upload_id = tools.nowString(),
            datasource = CONST.DATASOURCE_NAME,
            groups = groups,
            is_first_page = True,
            is_last_page = True,
            force_restart_upload = True
    )

    if CONST.getDebug() or CONST.isVerbose():
        print("\n--== groups request ==--\n", bulkRequest)
        print("\n")

    if not CONST.getDebug():
        try:
            if CONST.isVerbose():
                print("making groups API call.\n")
            permissions_api_client.bulkindexgroups_post(bulkRequest)
        except indexing_api.ApiException as e:
            print("Exception when calling PermissionApi->bulkindexgroups_post: %s\n" % e)


#
# Indexing Memberships
#
def indexMemberships(groups, memberships):

    print("indexing memberships")

    for group in groups:
        print("group:", group)
        groupMemberships = []

        for membership in memberships:
            if membership['group'] == group:
                groupMemberships.append(DatasourceBulkMembershipDefinition(
                    member_user_id = membership['email']
                ))

        bulkMembersRequest =  BulkIndexMembershipsRequest(
            upload_id = tools.nowString(),
            datasource = CONST.DATASOURCE_NAME,
            memberships =  groupMemberships,
            is_first_page = True,
            is_last_page = True,
            force_restart_upload = True,
            group = group
        )


        if CONST.getDebug() or CONST.isVerbose():
            print(f"\n--== bulk membership request for [{group}] ==--\n", bulkMembersRequest)
            print("\n")

        if not CONST.getDebug():
            try:
                if CONST.isVerbose():
                    print("making bullkindexmembership API call.\n")
                permissions_api_client.bulkindexmemberships_post(bulkMembersRequest)
            except indexing_api.ApiException as e:
                print("Exception when calling PermissionApi->indexuser_post: %s\n" % e)



#
# Indexing the People
#
def indexPeople(people):
    peopleCounter = 0
    allUsers = []

    # Making sure one user is imported, the specified datasource owner
    allUsers.append(DatasourceUserDefinition(
            name = CONST.DATASOURCE_USER,
            email = CONST.DATASOURCE_EMAIL,
            user_id = "001",
            is_active = True
        )
    )

    for person in people:
        peopleCounter += 1
        if peopleCounter >= CONST.MIN_IDX and peopleCounter <= CONST.MAX_IDX:
            if person.email:

                isActive = person.is_suspended == False

                user = DatasourceUserDefinition(
                    name = person.name,
                    email = person.email,
                    is_active = isActive,
                    user_id = person.id
                )

                if CONST.isVerbose():
                    print(f"Adding person: {person.email} to user list.\n")

                allUsers.append(user)
            else:
                print(f"Skipping user: {person.id}")
                errorList.append(person.id)
                continue

    bulkUserRequest = BulkIndexUsersRequest(
        upload_id = tools.nowString(),
        datasource = CONST.DATASOURCE_NAME,
        users = allUsers,
        is_first_page = True,
        is_last_page = True,
        force_restart_upload = True
    )


    if CONST.getDebug() or CONST.isVerbose():
        print("\n--== user request ==--\n", bulkUserRequest)
        print("\n")

    if not CONST.getDebug():
        try:
            if CONST.isVerbose():
                print("making permissions API call.\n")
            permissions_api_client.bulkindexusers_post(bulkUserRequest)
        except indexing_api.ApiException as e:
            print("Exception when calling PermissionApi->indexuser_post: %s\n" % e)




#
#   M A I N
#

# Configure data source just in case
configureDatasource()


#
# Indexing Content
if CONST.CRAWL_TYPE == "FULL" or CONST.CRAWL_TYPE == "CONTENT":
    print("\n-=-=-= CONTENT CRAWL =-=-=-")
    entries = benchling.getEntries()

    # To index one directly
    if CONST.DIRECT_ID != None:
        for entry in entries:
            if entry['id'] == CONST.DIRECT_ID:
                entries = [ entry ]

    print(f"total pages: {len(entries)}\n")

    #print(f"Entries", entries)

    indexDocs(entries)


#
# Indexing all the users
if CONST.CRAWL_TYPE == "FULL" or CONST.CRAWL_TYPE == "ENTITY":
    print("\n-=-=-= ENTITY CRAWL =-=-=-")

    people = benchling.getUsers()

    # To index one directly
    if CONST.DIRECT_ID != None:
        for user in people:
            if user['email'] == CONST.DIRECT_ID:
                people = [ user ]

    print(f"total users: {len(people)}\n")

    #indexGroups(groups)
    indexPeople(people)

print("The Error List: ", errorList)