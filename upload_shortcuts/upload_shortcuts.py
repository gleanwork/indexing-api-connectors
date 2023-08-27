import argparse
import csv
import logging
import time
import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.model.upload_shortcuts_request import UploadShortcutsRequest
from glean_indexing_api_client.model.shortcut import Shortcut
from glean_indexing_api_client.api import shortcuts_api

# Constants
BEARER_TOKEN = '<Your Glean Api Key>'
GLEAN_DOMAIN = '<Your Glean Domain>'
BATCH_SIZE = 1000

# Configure host and bearer authorization
configuration = indexing_api.Configuration(
    host = f"https://{GLEAN_DOMAIN}-be.glean.com/api/index/v1",
    access_token = BEARER_TOKEN)
api_client = indexing_api.ApiClient(configuration)

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv(file_name):
    """
    Reads shortcuts from a CSV file and returns them as a list of dictionaries.
    """
    shortcuts = []
    try:
        with open(file_name, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                shortcut_data = {
                    'input_alias': row.get('input_alias') + str(int(time.time())),
                    'destination_url': row.get('destination_url'),
                    'created_by': row.get('created_by')
                }

                # Add optional fields only if they exist and are not empty
                if row.get('url_template', '').strip():
                    shortcut_data['url_template'] = row.get('url_template')

                if row.get('description', '').strip():
                    shortcut_data['description'] = row.get('description')

                if 'unlisted' in row and row['unlisted'].strip():
                    shortcut_data['unlisted'] = row.get('unlisted', '').lower() == 'true'

                if row.get('create_time', '').strip():
                    shortcut_data['create_time'] = int(row['create_time'])

                if row.get('update_time', '').strip():
                    shortcut_data['update_time'] = int(row['update_time'])

                if row.get('updated_by', '').strip():
                    shortcut_data['updated_by'] = row['updated_by']

                shortcuts.append(Shortcut(**shortcut_data))

        logging.info(f"{len(shortcuts)} shortcuts successfully read from the CSV: {file_name}.")
        
    except Exception as e:
        logging.error(f"Error reading CSV: {e}")

    return shortcuts

def upload_shortcuts(upload_id, shortcuts):
    """
    Uploads shortcuts to the Glean platform in batches.
    """
    total_shortcuts = len(shortcuts)
    for i in range(0, total_shortcuts, BATCH_SIZE):
        batch = shortcuts[i:i + BATCH_SIZE]
        api = shortcuts_api.ShortcutsApi(api_client)
        
        try:
            api.uploadshortcuts_post(UploadShortcutsRequest(
                upload_id = upload_id,
                shortcuts = batch,
                is_first_page = i == 0,
                is_last_page = i + BATCH_SIZE >= total_shortcuts,
                force_restart_upload=False
            ))
        except indexing_api.ApiException as e:
            logging.error("Exception while uploading shortcuts: %s" % e.body)
            exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Upload shortcuts from a CSV file to Glean in batches.")
    parser.add_argument('csv_path', type=str, help="Path to the CSV file containing shortcuts.")
    args = parser.parse_args()

    current_time = int(time.time())
    upload_id = f'upload-shortcuts-{current_time}'
    logging.info(f"Starting upload process with ID: {upload_id}")

    shortcuts_list = read_csv(args.csv_path)
    upload_shortcuts(upload_id, shortcuts_list)
    
    logging.info("Upload process completed!")
