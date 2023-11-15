import glean_indexing_api_client
from glean_indexing_api_client.api import permissions_api
from glean_indexing_api_client.model.greenlist_users_request import GreenlistUsersRequest
from constants import DATASOURCE_NAME, API_CLIENT

def main(email: str):
    permission_api = permissions_api.PermissionsApi(API_CLIENT)
    greenlist_users_request = GreenlistUsersRequest(
        datasource=DATASOURCE_NAME,
        emails=[
            email
        ]
    ) 

    try:
        permission_api.betausers_post(greenlist_users_request)
    except glean_indexing_api_client.ApiException as e:
        print("Exception when calling PermissionsApi->betausers_post: %s\n" % e)

if __name__ == "__main__":
    email = input('email: ')
    main(email)