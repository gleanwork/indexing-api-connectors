import constants

def list_notebooks():
    # Send a request to list notebooks
    params = {
        'path': '/Users/alexis.deschamps@databricks.com/example_folder_1'
    }
    response = constants.send_request("/api/2.0/workspace/list", params=params)
    print(response)

    notebooks = response["objects"]
    print("Notebooks in the workspace:")
    for notebook in notebooks:
        print(notebook["path"])

        
list_notebooks()