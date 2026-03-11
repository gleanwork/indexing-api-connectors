# Coupa Connector

A Glean indexing connector for [Coupa](https://www.coupa.com/) that fetches and indexes the following object types into Glean:

- **Contracts** - contract name, description, start/end dates, status, and author information
- **Suppliers** - supplier name, tax ID, status, and address metadata (country, state, city)
- **Legal Entities** - entity name, abbreviation, and address details

The connector uses the Coupa REST API with OAuth2 client credentials authentication and pushes documents to Glean via the [Indexing API](https://developers.glean.com/docs/indexing_api_getting_started/).

## Prerequisites

- Python 3
- A Coupa instance with API access (OAuth2 client credentials)
- A Glean instance with a Push API token

## Setup

1. Initialize the Python virtual environment:

   ```bash
   ./init.sh
   ```

2. Activate the virtual environment:

   ```bash
   source ./__coupa__/bin/activate
   ```

3. Install dependencies:

   ```bash
   ./install.sh
   ```

4. Create your `.env` file by copying the example and filling in your credentials:

   ```bash
   cp _.env-example .env
   ```

   Edit `.env` and configure the following:

   | Variable | Description |
   |---|---|
   | `COUPA_BASE_URL` | Your Coupa instance URL (e.g., `https://yourcompany.coupahost.com`) |
   | `COUPA_CLIENT_ID` | OAuth2 client ID |
   | `COUPA_CLIENT_SECRET` | OAuth2 client secret |
   | `COUPA_SCOPES` | OAuth2 scopes (see `_.env-example` for required scopes) |
   | `GLEAN_INSTANCE` | Your Glean instance name |
   | `GLEAN_PUSH_API_TOKEN` | Glean Push API token |
   | `DATASOURCE_NAME` | Datasource identifier in Glean (e.g., `coupa`) |
   | `DATASOURCE_DISPLAYNAME` | Display name shown in Glean (e.g., `Coupa`) |
   | `DATASOURCE_HOMEURL` | Your Coupa instance URL |
   | `DATASOURCE_URLREGEX` | URL regex for matching Coupa URLs |
   | `DATASOURCE_ICON` | URL for the datasource icon |
   | `DATASOURCE_VIEWURLBASE` | Base URL for constructing document view links |
   | `DEBUG` | Set to `true` to run in debug mode (no API calls made) |
   | `BULK_INDEX` | Set to `true` to use bulk indexing |
   | `BULK_BATCH_SIZE` | Number of documents per bulk index batch (default: `100`) |

## Usage

Run the connector:

```bash
./run.sh
```

### Command-line options

The connector supports the following optional flags:

| Flag | Description |
|---|---|
| `-d`, `--debug` | Enable/disable debug mode |
| `-v`, `--verbose` | Enable verbose logging |
| `-c`, `--crawl-type` | Crawl type: `full`, `entity`, `content`, or `none` |
| `-m`, `--min-idx` | Minimum index offset |
| `-x`, `--max-idx` | Maximum index offset |
| `-r`, `--rate` | API request rate (requests per second) |
| `-t`, `--threads` | Number of threads |
| `-p`, `--public-only` | Only index public documents |

Example:

```bash
./run.sh -v -d false
```

## File Structure

| File | Description |
|---|---|
| `coupa.py` | Main connector logic - fetches data from Coupa APIs |
| `indexer.py` | Handles Glean datasource configuration and document indexing |
| `gleanConstants.py` | Runtime configuration loaded from environment variables |
| `gleantools.py` | Utility functions (CSV handling, date conversion) |
| `init.sh` | Creates the Python virtual environment |
| `install.sh` | Installs Python dependencies |
| `run.sh` | Activates the environment and runs the connector |
