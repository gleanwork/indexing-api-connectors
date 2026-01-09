#!python3
"""Glean Slab Crawler

Thank you for using the Glean Slab Crawler example

This script will help you crawl a set of URLs from a csv and import them
into a datasource.


---------------------------
| Hit any key to continue |
---------------------------

"""

#
# Imports
#
import os, sys, json, time, copy
import gleantools as tools
import gleanConstants as Constants
import slab as Slab

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
from glean_indexing_api_client.model.bulk_index_documents_request import BulkIndexDocumentsRequest
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
slab = Slab.Slab()

if CONST.getDebug() or CONST.isVerbose():
    print("Batch size: ", CONST.BATCH_SIZE)
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

    slabTopicProp = PropertyDefinition(
        name = "slabTopic",
        display_label = "Topic",
        display_label_plural = "Topics",
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

    postObj = ObjectDefinition(
        name = "post",
        display_label = "Post",
        # doc_category = "UNCATEGORIZED" | "TICKETS" | "CRM" | "PUBLISHED_CONTENT" | "COLLABORATIVE_CONTENT" |
        #                "QUESTION_ANSWER" | "MESSAGING" | "CODE_REPOSITORY" | "CHANGE_MANAGEMENT" | "PEOPLE" |
        #                "EMAIL" | "SSO" | "ATS" | "KNOWLEDGE_HUB" | "EXTERNAL_SHORTCUT",
        property_definitions = [ slabTopicProp ],
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
        is_user_referenced_by_email = True,
        name = CONST.DATASOURCE_NAME,
        object_definitions = [ postObj ],
        # product_access_group = str,
        # quicklinks = [ Quicklink ]
        # redlist_title_regex = str,
        render_config_preset = "gdrive",
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
# Build full document
#
def buildDocument(post):

    #print(f"Building Document - id: {post['id']} - title: {post['title']}")

    doc = slab.getPostDetails(post)

    folderId = doc['container']
    #projectId = getProjectFromFolder(folderId)

    allowedUserList = [ UserReferenceDefinition(email = doc['author']) ]
    if (doc['author'] != doc['updated_by'] != doc['author']):
        allowedUserList.append(UserReferenceDefinition(email = doc['updated_by']))

    #allowedUserList.append(UserReferenceDefinition(email = "steve.smith@salessavvy.net"))

    if CONST.PUBLIC_ONLY == True:
        #docPermissions['allow_anonymous_access'] = True
        docPermissions = DocumentPermissionsDefinition(
            allow_anonymous_access = True
        )
    else:
        docPermissions = DocumentPermissionsDefinition(
            allowed_users = allowedUserList
        )

        if doc['private'] != True:
            docPermissions['allow_all_datasource_users_access'] = True


    # building document request JSON
    document = DocumentDefinition(
        #additional_urls =
        author = UserReferenceDefinition(email = doc['author']),
        body = ContentDefinition(**doc['body']),
        container = doc['container'],
        created_at = doc['created_at'],
        custom_properties = [ CustomProperty(name="slabTopic", value=doc['topic']) ],
        #                      CustomProperty(name="slabFolder", value=getFolderNameFromId(folderId)) ],
        datasource = CONST.DATASOURCE_NAME,
        id = doc['id'],
        #interactions =
        object_type = "post",
        #owner =
        permissions = docPermissions,
        #status =
        #summary =
        #tags =
        title = doc['title'],
        updated_at = doc['updated_at'],
        updated_by = UserReferenceDefinition(email = doc['updated_by']),
        view_url = doc['view_url']
    )

    return document



#
# Bulk index posts batching by size
# 
# input: an array of posts with at least an 'id' attribute
# process: iterate through posts, fetch the metadata w/content, batch upload to Glean
#
def indexPosts(posts):

    uploadId = tools.nowString()
    counter = 0
    breaker = False
    currenBatch = []
    batchNum = 0
    batchSize = 0
    totalPosts = len(posts)
    backoff = 3

    print(f"uploadId: {uploadId} - Starting Bulk Document Upload")

    startBatch = BulkIndexDocumentsRequest(
            upload_id = uploadId,
            datasource = CONST.DATASOURCE_NAME,
            documents = [],
            is_first_page = True,
            is_last_page = False,
            force_restart_upload = True
    )

    if not CONST.getDebug():
        try:
            documents_api_client.bulkindexdocuments_post(startBatch)
        except indexing_api.ApiException as e:
            print("Exception when calling DocumentsApi->bulkindexdocuments_post: %s\n" % e)


    while counter < totalPosts and breaker == False:
        if counter > 1: time.sleep(CONST.DELAY)

        post = posts[counter]

        try: 
            document = buildDocument(post)
            counter += 1
        except:
            print(f"uploadId: {uploadId} - idx {counter}/{totalPosts} - ERROR retrieving id {post['id']} - title {post['title']} - retrying")
            time.sleep(backoff)
            continue

        currenBatch.append(document)

        postSize = 100   # header markup
        try:
            postSize += len(document['body']['text_content'])
        except:
            try:
                postSize += len(document['body']['binary_content'])
            except:
                postSize += 100
        batchSize += postSize

        print(f"uploadId: {uploadId} - idx {counter}/{totalPosts} - size {batchSize}/{CONST.BATCH_SIZE} - id {post['id']} - title {post['title']}")

        if batchSize >= CONST.BATCH_SIZE or counter == totalPosts:
            batchNum += 1
            print(f"uploadId: {uploadId} - Uploading batch {batchNum} containing {len(currenBatch)} posts")
            uploadBatch = BulkIndexDocumentsRequest(
                    upload_id = uploadId,
                    datasource = CONST.DATASOURCE_NAME,
                    documents = currenBatch,
                    is_first_page = False,
                    is_last_page = False,
            ) 

            if not CONST.getDebug():
                try:
                    documents_api_client.bulkindexdocuments_post(uploadBatch)
                except indexing_api.ApiException as e:
                    print("Exception when calling DocumentsApi->bulkindexdocuments_post: %s\n" % e)

            batchSize = 0
            currenBatch = []

    endBatch = BulkIndexDocumentsRequest(
            upload_id = uploadId,
            datasource = CONST.DATASOURCE_NAME,
            documents = [],
            is_first_page = False,
            is_last_page = True,
    )
    print(f"uploadId: {uploadId} - Ending Bulk Document Upload")
    if not CONST.getDebug():
        try:
            documents_api_client.bulkindexdocuments_post(endBatch)
        except indexing_api.ApiException as e:
            print("Exception when calling DocumentsApi->bulkindexdocuments_post: %s\n" % e)



def updatePosts(posts):

    backoff = 3
    breaker = False
    counter = 0
    totalPosts = len(posts)

    while counter < totalPosts and breaker == False:
        if counter > 1: time.sleep(CONST.DELAY)

        post = posts[counter]

        try: 
            document = buildDocument(post)
            counter += 1
        except:
            print(f"Importing Document: {counter}/{totalPosts} - ERROR retrieving id {post['id']} - title {post['title']} - retrying")
            time.sleep(backoff)
            continue

        if counter >= CONST.MIN_IDX and counter <= CONST.MAX_IDX and document['id']:
            if counter > 1: time.sleep(CONST.DELAY)
            print(f"Importing Document: {counter}/{totalPosts} - id: {document['id']} - title: {document['title']}")

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
    if CONST.DATASOURCE_EMAIL != "":
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
            if person['email'] != None:

                isActive = person['deactivatedAt'] == None

                user = DatasourceUserDefinition(
                    name = person['name'],
                    email = person['email'],
                    is_active = isActive,
                    user_id = person['id']
                )

                if CONST.isVerbose():
                    print(f"Adding person: {person['email']} to user list")

                allUsers.append(user)
            else:
                print(f"Skipping user: {person['email']}")
                errorList.append(person['email'])
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
if CONST.CRAWL_TYPE in [ "FULL", "CONTENT", "INCREMENTAL"]:
    print("\n-=-=-= CONTENT CRAWL =-=-=-")
    posts = slab.getAllPosts()

    filteredPosts = []
    postCount = 0
    archivedCount = 0

    # To index one directly
    if CONST.DIRECT_ID != None:
        for post in posts:
            if post['id'] == CONST.DIRECT_ID:
                filteredPosts = [ post ]

    else:
        for post in posts:
            postCount += 1
            if post['archivedAt'] == None:
                filteredPosts.append(post)
            else:
                archivedCount += 1

            if postCount >= CONST.MAX_IDX:
                breaker = True
                break

    print(f"Total posts: {len(filteredPosts)} - archived {archivedCount}\n")

    if CONST.CRAWL_TYPE == "INCREMENTAL":
        updatePosts(filteredPosts)
    else:
        indexPosts(filteredPosts)


#
# Indexing all the users
if CONST.CRAWL_TYPE == "FULL" or CONST.CRAWL_TYPE == "ENTITY":
    print("\n-=-=-= ENTITY CRAWL =-=-=-")

    people = slab.getUsers()

    filteredPeople = []
    personCount = 0
    deactivated = 0

    # To index one directly
    if CONST.DIRECT_ID != None:
        for person in people:
            if person['email'] == CONST.DIRECT_ID:
                people = [ person ]

    else:
        for person in people:
            personCount += 1

            if personCount <= CONST.MAX_IDX:
                if person['deactivatedAt'] == None:
                    filteredPeople.append(person)
                else:
                    deactivated += 1
            else:
                break

    print(f"Total people: {len(filteredPeople)} - deactivated: {deactivated}\n")

    #indexGroups(groups)
    indexPeople(people)

print("The Error List: ", errorList)