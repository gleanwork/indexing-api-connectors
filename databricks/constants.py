import glean_indexing_api_client as indexing_api
import os

DATASOURCE_NAME = "databricks"
DASHBOARD_OBJECT_NAME = "Dashboard"

configuration = indexing_api.Configuration(
    host="https://databricks-be.glean.com/api/index/v1",
    access_token=os.getenv("GLEAN_API_KEY"))

API_CLIENT = indexing_api.ApiClient(configuration)
