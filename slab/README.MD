# Glean Slab Connector

A Python-based connector that indexes content from [Slab](https://slab.com/) into Glean's search platform. This connector uses Slab's GraphQL API to fetch posts, topics, and users, then indexes them into Glean using the Indexing API.

## Overview

The Slab connector provides the following functionality:

- **Content Indexing**: Crawls and indexes all posts from your Slab workspace
- **User Management**: Indexes all users with their permissions and access levels
- **Topic Organization**: Maps Slab topics to Glean containers for better organization
- **Permission Handling**: Respects Slab's privacy settings and user permissions
- **Bulk Operations**: Efficiently processes large amounts of data using Glean's bulk indexing API

## Features

- **Full Crawl**: Index all posts and users
- **Incremental Updates**: Update existing documents
- **Content Processing**: Extracts text content from Slab's rich content format
- **Topic Hierarchy**: Preserves Slab's topic structure in Glean
- **Rate Limiting**: Configurable request delays to respect API limits
- **Debug Mode**: Comprehensive logging for troubleshooting

## Prerequisites

- Python 3.7 or higher
- Access to a Slab workspace with API token
- Glean instance with Indexing API access
- Valid Glean API token

## Installation

### 1. Initialize the Environment

```bash
./init.sh
source ./__slab__/bin/activate
./install.sh
```

### 2. Configure Environment Variables

```bash
cp _.env-example .env
```

Edit the `.env` file with your specific configuration:

```bash
# Required: Your Glean instance details
GLEAN_INSTANCE="your-glean-instance"
GLEAN_PUSH_API_TOKEN="your-glean-api-token"

# Required: Your Slab API credentials
SLAB_API_URL="https://api.slab.com"
SLAB_TOKEN="your-slab-api-token"

# Required: Datasource configuration
DATASOURCE_NAME="slab-connector"
DATASOURCE_DISPLAYNAME="Slab Knowledge Base"
DATASOURCE_CATEGORY="PUBLISHED_CONTENT"
DATASOURCE_EMAIL="admin@yourcompany.com"
DATASOURCE_USER="Slab Admin"
DATASOURCE_HOMEURL="https://yourcompany.slab.com"
DATASOURCE_VIEWURLBASE="https://yourcompany.slab.com/"
DATASOURCE_URLREGEX="^https://yourcompany\\.slab\\.com.*"
DATASOURCE_ICON="https://slabstatic.com/prod/uploads/7cd8fe75/logos/thumb/TEFVMSF-M5roG65K3ftuRNJB.png"

# Optional: Debug settings
DEBUG="false"
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GLEAN_INSTANCE` | Yes | Your Glean instance name (without the domain) |
| `GLEAN_PUSH_API_TOKEN` | Yes | Glean API token for indexing |
| `SLAB_API_URL` | Yes | Slab API base URL (usually `https://api.slab.com`) |
| `SLAB_TOKEN` | Yes | Slab API token for authentication |
| `DATASOURCE_NAME` | Yes | Unique identifier for the datasource in Glean |
| `DATASOURCE_DISPLAYNAME` | Yes | Human-readable name for the datasource |
| `DATASOURCE_CATEGORY` | Yes | Content category (e.g., `PUBLISHED_CONTENT`) |
| `DATASOURCE_EMAIL` | Yes | Admin email for the datasource |
| `DATASOURCE_USER` | Yes | Admin user name for the datasource |
| `DATASOURCE_HOMEURL` | Yes | Base URL for the Slab workspace |
| `DATASOURCE_VIEWURLBASE` | Yes | Base URL for viewing documents |
| `DATASOURCE_URLREGEX` | Yes | Regex pattern for matching Slab URLs |
| `DATASOURCE_ICON` | Yes | Icon URL for the datasource |
| `DEBUG` | No | Enable debug mode (`true`/`false`) |

### Getting Slab API Token

1. Log into your Slab workspace
2. Go to **Settings** → **API**
3. Generate a new API token
4. Copy the token to your `.env` file

## Usage

### Basic Usage

Run the connector with default settings:

```bash
./run.sh
```

### Advanced Options

The connector supports various command-line options:

```bash
./run.sh [OPTIONS]
```

#### Available Options

| Option | Description | Example |
|--------|-------------|---------|
| `-v, --verbose` | Enable verbose logging | `./run.sh -v` |
| `-d, --debug` | Enable debug mode | `./run.sh -d true` |
| `-c, --crawl-type` | Set crawl type | `./run.sh -c FULL` |
| `-r, --rate` | Set request rate (requests/second) | `./run.sh -r 0.1` |
| `-b, --batch-size` | Set batch size in bytes | `./run.sh -b 1048576` |
| `-i, --id` | Index specific document by ID | `./run.sh -i 4z34v46a` |
| `-p, --public-only` | Index only public documents | `./run.sh -p` |
| `--min-idx` | Start indexing from specific index | `./run.sh --min-idx 10` |
| `--max-idx` | Stop indexing at specific index | `./run.sh --max-idx 100` |
| `-t, --threads` | Set maximum threads | `./run.sh -t 5` |

#### Crawl Types

- `FULL`: Index all content and users (default)
- `CONTENT`: Index only content/posts
- `ENTITY`: Index only users
- `INCREMENTAL`: Update existing documents
- `NONE`: Skip indexing (useful for testing)

### Usage Examples

#### Index with Verbose Logging and Rate Limiting

```bash
./run.sh -v -r 0.1
```

#### Index Specific Document

```bash
./run.sh -v -i 4z34v46a
```

#### Index Only Public Documents

```bash
./run.sh -v -p
```

#### Partial Indexing (Records 10-50)

```bash
./run.sh -v --min-idx 10 --max-idx 50
```

#### Debug Mode with Custom Batch Size

```bash
./run.sh -d true -b 2097152 -v
```

## How It Works

### 1. Authentication & Setup

The connector authenticates with Slab using your API token and discovers your workspace's host URL.

### 2. Content Crawling

- Fetches all posts from your Slab workspace
- Retrieves detailed content for each post
- Extracts text content from Slab's rich content format
- Maps topics and permissions

### 3. User Indexing

- Fetches all users from your Slab workspace
- Maps user permissions and access levels
- Indexes user information for Glean's permission system

### 4. Document Processing

- Converts Slab posts to Glean document format
- Preserves metadata (author, creation date, topics)
- Maps Slab permissions to Glean access controls
- Handles both public and private content

### 5. Bulk Indexing

- Uses Glean's bulk indexing API for efficiency
- Processes documents in configurable batch sizes
- Provides progress tracking and error handling

## Data Mapping

### Slab → Glean Mapping

| Slab Field | Glean Field | Description |
|------------|-------------|-------------|
| `post.title` | `document.title` | Post title |
| `post.content` | `document.body.text_content` | Post content (text extracted) |
| `post.owner.email` | `document.author` | Post author |
| `post.publishedAt` | `document.created_at` | Creation timestamp |
| `post.updatedAt` | `document.updated_at` | Last modified timestamp |
| `post.topics[0].name` | `document.custom_properties.slabTopic` | Primary topic |
| `post.topics[0].hierarchy` | `document.container` | Topic hierarchy path |

### Permission Mapping

- **Public Topics**: Documents are accessible to all Glean users
- **Private Topics**: Documents are restricted to topic members
- **User Permissions**: Mapped to Glean's user reference system

## Troubleshooting

### Common Issues

#### Authentication Errors

```
Error in getting host data from API - problem with auth token?
```

**Solution**: Verify your `SLAB_TOKEN` is correct and has proper permissions.

#### API Rate Limiting

If you encounter rate limiting, reduce the request rate:

```bash
./run.sh -r 0.05  # 1 request every 20 seconds
```

#### Memory Issues

For large workspaces, reduce batch size:

```bash
./run.sh -b 524288  # 512KB batches
```

#### Debug Mode

Enable debug mode for detailed logging:

```bash
./run.sh -d true -v
```

### Log Files

The connector outputs detailed logs to stdout. For persistent logging, redirect output:

```bash
./run.sh -v > slab_indexing.log 2>&1
```

## Performance Considerations

- **Batch Size**: Default 3MB, adjust based on your content size
- **Rate Limiting**: Default 0.2s delay between requests
- **Threading**: Default 10 threads, reduce if experiencing issues
- **Memory**: Monitor memory usage for large workspaces

## Security

- API tokens are stored in environment variables
- No sensitive data is logged in debug mode
- SSL verification is enabled for all API calls
- User permissions are preserved during indexing

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Enable debug mode for detailed error information
3. Review the logs for specific error messages
4. Verify your API tokens and permissions

## License

This connector is part of the Glean Indexing API Connectors project.