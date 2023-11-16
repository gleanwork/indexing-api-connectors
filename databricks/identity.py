from constants import send_request, DATASOURCE_NAME, API_CLIENT
import logging
import collections
from typing import List
from glean_indexing_api_client.api import permissions_api
from glean_indexing_api_client.model.index_group_request import IndexGroupRequest
from glean_indexing_api_client.model.index_user_request import IndexUserRequest
from glean_indexing_api_client.model.index_membership_request import IndexMembershipRequest
from glean_indexing_api_client.model.bulk_index_users_request import BulkIndexUsersRequest
from glean_indexing_api_client.model.datasource_user_definition import DatasourceUserDefinition
from glean_indexing_api_client.model.datasource_bulk_membership_definition import DatasourceBulkMembershipDefinition
from glean_indexing_api_client.model.bulk_index_groups_request import BulkIndexGroupsRequest
from glean_indexing_api_client.model.datasource_group_definition import DatasourceGroupDefinition
from glean_indexing_api_client.model.bulk_index_memberships_request import BulkIndexMembershipsRequest
import uuid


permissions_api_client = permissions_api.PermissionsApi(API_CLIENT)

example_users_response = """
{
  "schemas": [
    "urn:ietf:params:scim:api:messages:2.0:ListResponse"
  ],
  "totalResults": 10,
  "startIndex": 1,
  "itemsPerPage": 0,
  "Resources": [
    {
      "schemas": [
        "urn:ietf:params:scim:schemas:core:2.0:User"
      ],
      "id": "string",
      "userName": "user@example.com",
      "emails": [
        {
          "$ref": "string",
          "value": "string",
          "display": "string",
          "primary": true,
          "type": "string"
        }
      ],
      "name": {
        "givenName": "string",
        "familyName": "string"
      },
      "displayName": "string",
      "groups": [
        {
          "$ref": "string",
          "value": "string",
          "display": "string",
          "primary": true,
          "type": "string"
        }
      ],
      "roles": [
        {
          "$ref": "string",
          "value": "string",
          "display": "string",
          "primary": true,
          "type": "string"
        }
      ],
      "entitlements": [
        {
          "$ref": "string",
          "value": "string",
          "display": "string",
          "primary": true,
          "type": "string"
        }
      ],
      "externalId": "string",
      "active": true
    }
  ]
}
"""

def _upload_users(users: List[dict], upload_id: str):
    users_batch = []
    groups_batch = []
    group_to_memberships = collections.defaultdict(list)
    for user in users:
        primary_email = [email for email in user['emails'] if email['primary']][0]['value']
        user = DatasourceUserDefinition(
                name = user['displayName'],
                # TODO: Confirm whether username is always an email address
                email = primary_email,
                user_id = user['id'],
                is_active = user['active'],
            )
        
        users_batch.append(user)
        for group in user['groups']:
            group_name = group['display']
            groups_batch.append(DatasourceGroupDefinition(
                name = group_name
            ))
            group_to_memberships[group_name].append(DatasourceBulkMembershipDefinition(
                member_user_id = user['id'],
            ))
    users_request = BulkIndexUsersRequest(
            upload_id = upload_id,
            datasource = DATASOURCE_NAME,
            groups = users_batch,
            is_first_page = False,
            is_last_page = False,
            force_restart_upload = False
    )
    permissions_api_client.bulkindexusers_post(users_request)
    groups_request = BulkIndexGroupsRequest(
            upload_id = upload_id,
            datasource = DATASOURCE_NAME,
            groups = groups_batch,
            is_first_page = False,
            is_last_page = False,
            force_restart_upload = False
    )
    permissions_api_client.bulkindexgroups_post(groups_request)
    for k, v in group_to_memberships.items():
        members_request =  BulkIndexMembershipsRequest(
                upload_id = upload_id,
                datasource = DATASOURCE_NAME,
                memberships =  v,
                is_first_page = False,
                is_last_page = False,
                force_restart_upload = False,
                group = k
            )
        permissions_api_client.bulkindexmemberships_post(members_request)



def crawl_users():
    count = 100
    start_index = 1

    while True:
        params = {
            'startIndex': start_index,
            'count': count
        }
        response = send_request('/api/2.0/preview/scim/v2/Users', params=params)
        users = response['resources']

        # Generate uuid for upload id
        _upload_users(users, str(uuid.uuid4()))

        if len(users) < count:
            logging.info(f'Unfilled user page {start_index}, received: {len(users)}, count: {count}. Finishing crawl')
            break
        
        start_index += count
            

        

    

