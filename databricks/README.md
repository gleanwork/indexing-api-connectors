## Sample Wikipedia Connector using Glean's Indexing API

This is a sample connector that uses the Glean Indexing API to index Wikipedia articles. It is written in Python and uses [Wikipedia's MediaWiki Action API](https://www.mediawiki.org/wiki/Special:MyLanguage/API:Main_page) to fetch articles related to a search term.

The purpose of this sample connector is to demonstrate how to use the Glean Indexing API to index documents. It is not intended to be a production-ready connector.

### Steps to run the connector

1. Make sure you're in the `wikipedia` directory.

2. Run the setup script using the following command. This will create a virtual environment, and install the required dependencies.
```sh
$ ./setup.sh
```

3. Replace `<YOUR_GLEAN_DOMAIN>` and `<YOUR_GLEAN_API_KEY>` in `wikipedia_bulk_index.py` with your domain and API key respectively. You can follow the [Managing API tokens](https://developers.glean.com/docs/indexing_api_tokens/) tutorial for more details on how to generate an API key.

4. Run the connector using the following command. This will start indexing documents from Wikipedia.
```sh
$ python wikipedia_bulk_index.py
```

5. You're all set, you can soon search for the indexed documents from the search page in Glean! 
