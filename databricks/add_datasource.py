import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.api import datasources_api
from glean_indexing_api_client.model.custom_datasource_config import CustomDatasourceConfig
from glean_indexing_api_client.model.object_definition import ObjectDefinition
from constants import DASHBOARD_OBJECT_NAME, API_CLIENT

def add_datasource(datasource_name: str):
    """
    Inserts/updates the custom datasource configuration.
    (Preferably, should be done via Glean's workspace settings: https://app.glean.com/admin/setup/apps/)
    """
    
    datasource_config = CustomDatasourceConfig(
        name=datasource_name,
        display_name="Databricks Staging",
        datasource_category="KNOWLEDGE_HUB",
        url_regex="^https?://e2-dogfood-ext-glean-staging-1.staging.cloud.databricks.com/.*",
        is_test_datasource=True,  # Switch to false for production
        object_definitions=[
            ObjectDefinition(
                doc_category='KNOWLEDGE_HUB',
                name=DASHBOARD_OBJECT_NAME),
            # TODO: Add object definitions for notebooks and tables
                ]
    )

    try:
        # Create datasources API instance
        datasource_api = datasources_api.DatasourcesApi(API_CLIENT)
        datasource_api.adddatasource_post(datasource_config)
        print("Datasource configuration created/updated successfully.")
    except indexing_api.ApiException as e:
        print(
            "Exception when calling DatasourcesApi->adddatasource_post: %s\n" %
            e.body)
        exit(1)
