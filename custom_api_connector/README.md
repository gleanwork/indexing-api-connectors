# Custom API Connector

A generic connector that fetches data from any REST API endpoint (with Bearer token auth) and pushes it to Glean via the Indexing API.

## Setup

### 1. Configure `config.json`

Edit `config.json` to match your source API and Glean datasource:

| Field | Description |
|---|---|
| `datasource_name` | Unique name for your Glean datasource |
| `datasource_display_name` | Display name shown in Glean |
| `datasource_category` | Category (e.g. `PUBLISHED_CONTENT`, `KNOWLEDGE_HUB`) |
| `object_type` | Document object type |
| `url_regex` | Regex matching your document URLs |
| `is_test_datasource` | Set to `false` for production |
| `source_api.endpoint` | The API URL to fetch data from |
| `source_api.method` | `GET` or `POST` |
| `source_api.headers` | Extra headers (auth is handled separately) |
| `source_api.params` | Query params (GET) or JSON body (POST) |
| `source_api.results_key` | JSON key containing the array of results (omit if the response is already an array) |
| `field_mappings.id` | Source field for document ID |
| `field_mappings.title` | Source field for document title |
| `field_mappings.body` | Source field for document body/content |
| `field_mappings.view_url` | Source field for document URL |
| `field_mappings.mime_type` | MIME type for body content (default: `text/html`) |

### 2. Set environment variables

| Variable | Description |
|---|---|
| `GLEAN_DOMAIN` | Your Glean domain (e.g. `mycompany` for `mycompany-be.glean.com`) |
| `GLEAN_API_TOKEN` | Glean Indexing API token (from Workspace Settings → API Tokens) |
| `SOURCE_API_TOKEN` | Bearer token for your source API |

### 3. Run locally

```bash
pip install -r requirements.txt

export GLEAN_DOMAIN="your-domain"
export GLEAN_API_TOKEN="your-glean-token"
export SOURCE_API_TOKEN="your-source-api-token"

python custom_api_connector.py
```

## GitHub Actions

The workflow at `.github/workflows/custom_api_connector.yml` runs this connector on a daily cron schedule.

### Required GitHub Secrets

Add these in your repo → Settings → Secrets and variables → Actions:

- `GLEAN_DOMAIN`
- `GLEAN_API_TOKEN`
- `SOURCE_API_TOKEN`

The workflow also supports manual runs via the "Run workflow" button in the Actions tab.
