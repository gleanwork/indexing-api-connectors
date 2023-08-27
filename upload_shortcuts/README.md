# Shortcuts Upload Script

This script allows users to upload shortcuts in bulk to Glean via the indexing API, using a CSV file as the source.

## CSV Format

The CSV should contain the following columns:

- `url_template`: (String) For variable shortcuts, this contains the URL template. Note that destination_url contains the default URL. This field can be left empty.
- `input_alias`: (String, Required) The link text following the viewPrefix as entered by the user. For example, if the view prefix is `go/` and the shortened URL is `go/abc`, then `abc` is the inputAlias.
- `description`: (String) A short, plain text blurb to help people understand the intent of the shortcut.
- `destination_url`: (String, Required) The destination URL for the shortcut.
- `created_by`: (String, Required) Email of the user who created this shortcut.
- `updated_by`: (String) Email of the user who last updated this shortcut.
- `unlisted`: (Boolean) Whether this shortcut is unlisted or not. Unlisted shortcuts are visible to the author and admins only.
- `create_time`: (Integer) The time the shortcut was created, represented in epoch seconds.
- `update_time`: (Integer) The time the shortcut was last updated, represented in epoch seconds.

> **Note**: Rows without required fields will cause the script to fail.

## Usage

1. Make sure you're in the `upload_shortcuts` directory.

2. Run the setup script using the following command. This will create a virtual environment, and install the required dependencies.
    ```sh
    $ ./setup.sh
    ```

3. Replace `<GLEAN_DOMAIN>` and `<BEARER_TOKEN>` in `upload_shortcuts.py` with your domain and API key respectively. You can follow the [Managing API tokens](https://developers.glean.com/docs/indexing_api_tokens/) tutorial for more details on how to generate an API key.
    
4. To run the script, execute the following command:
    ```bash
    python upload_shortcuts.py path_to_csv_file.csv
    ```

Replace `path_to_csv_file.csv` with the path to your actual CSV file.

## Constants

- **GLEAN_DOMAIN**: The domain for the Glean APIs.
- **BEARER_TOKEN**: Bearer token for authentication.
- **BATCH_SIZE**: Number of shortcuts to process in a single API call. The default is set to 1000.

## API Documentation

For more details about the API endpoint, refer to the [official Glean documentation](https://developers.glean.com/indexing/tag/Shortcuts/paths/~1uploadshortcuts/post/).

