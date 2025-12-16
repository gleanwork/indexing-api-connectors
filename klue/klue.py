# pylint: disable-all
import requests, time, json, os, logging
import gleanConstants as Constants
import gleanTools as tools
import gleanApi as glean
from urllib.parse import urljoin

CONST = Constants.Constants()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

attributes = ['id', 'title', 'bodyHtml', 'bodyMarkdown', 'topic', 'competitor',
              'competitorGroups', 'battlecards', 'audiences', 'tags', 'cardUrl',
              'author', 'sources', 'createdAt', 'updatedAt', 'reviewedAt']

class Klue:
    apiEndpoint = "https://app.klue.com/extract/cards.json"
    authToken = None
    lastTokenTime = None
    session = requests.Session()

    def __init__(self) -> None:
        self.lastTokenTime = 0
        self.get_auth_token()

    def apiRequest(self, apiPath="", retries=3, backoff_factor=1):
        """Make a request to the Klue API with retries."""
        self.get_auth_token()
        headers = {
            "Authorization": f"Bearer {self.authToken}",
            "Content-Type": "application/json"
        }
        url = urljoin(self.apiEndpoint, apiPath)
        if CONST.getDebug():
            logger.debug("Klue API Get - url: %s", url)
        
        for attempt in range(retries):
            try:
                response = self.session.get(url, headers=headers, timeout=10)
                response.raise_for_status()  # Raise exception for HTTP errors
                decodedResponse = response.json()
                
                logger.info("Response Status: %s", response.status_code)
                if "items" in decodedResponse:
                    return {"decodedResponse": decodedResponse, "headers": response.headers}
                
                logger.warning("Unexpected API Response Structure: %s", decodedResponse)
                return decodedResponse  # Return raw response for debugging
            
            except requests.exceptions.RequestException as e:
                logger.error("API call attempt %d failed: %s", attempt + 1, e)
                time.sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
        
        logger.critical("API request failed after %d attempts", retries)
        return None

    def convertKlueCardToGleanDoc(self, card: dict) -> dict:
        doc = {}
        doc['objectType'] = "battlecard"
        doc['datasource'] = CONST.DATASOURCE_NAME

        try:
            if 'competitor' in card:
                competitor = card['competitor']
                doc['container'] = competitor

                doc['customProperties'] = [
                    glean.CustomProperty(
                        name="competitor",
                        value=competitor
                    )
                ]

            if 'id' in card:
                doc['id'] = f"{card['id']}"

            if 'title' in card:
                proposedTitle = card['title']
                if competitor.lower() not in proposedTitle.lower():
                    doc['title'] = f"{competitor} - {card['title']}"
                else:
                    doc['title'] = card['title']

            if 'bodyHtml' in card:
                doc["body"] = glean.ContentDefinition(
                    text_content=card['bodyHtml'],
                    mime_type="text/html"
                )
            
            if 'cardUrl' in card:
                doc['view_url'] = card['cardUrl']

            doc['permissions'] = glean.DocumentPermissionsDefinition(
                allow_anonymous_access=True
            )
        except Exception as e:
            logger.error("Error converting card to doc: %s", e)
        return doc


    def getCards(self):
        """Fetch all cards from Klue API, paginating through results."""
        all_cards = []
        page_size = 100
        page_number = 1
        total_items = None
        
        while total_items is None or len(all_cards) < total_items:
            logger.info("Fetching page %d of Klue cards", page_number)
            response = self.apiRequest(f"?limit={page_size}&page={page_number}")
            if response and "decodedResponse" in response:
                data = response["decodedResponse"]
                if "items" in data:
                    all_cards.extend(data["items"])
                    logger.info("Fetched %d cards (Total so far: %d)", len(data["items"]), len(all_cards))
                if "totalItems" in data:
                    total_items = data["totalItems"]
                    logger.info("Total items to fetch: %d", total_items)
                page_number += 1
            else:
                logger.warning("Stopping pagination as no valid response received")
                break  # Stop if the response is invalid
        
        logger.info("Finished fetching all Klue cards. Total: %d", len(all_cards))
        return all_cards


    def getKlueCardsFromJsonFile(self, jsonFile: str) -> dict:
        exportData = tools.readFileData(jsonFile)
        allCards = []
        if 'items' in exportData:
            for item in exportData['items']:
                allCards.append(item)
        return allCards

    def get_auth_token(self):
        """Retrieve API token from environment."""
        if not self.authToken:
            self.authToken = CONST.KLUE_API_KEY
            if not self.authToken:
                raise ValueError("Missing KLUE_API_TOKEN environment variable.")
            if CONST.getDebug() or CONST.isVerbose():
                logger.debug("Klue authToken: %s", tools.maskString(self.authToken))
        return self.authToken

    def printCards(self, cards):
        if isinstance(cards, dict):
            cards = [cards]
        counter = 0
        for card in cards:
            counter += 1
            print(f"Card: {counter}")
            for attr in card:
                if attr in attributes:
                    if attr == 'bodyHtml' or attr == 'bodyMarkdown':
                        print(f"{attr}: {card[attr][:500]}")
                    else:
                        print(f"{attr}: {card[attr]}")
            print("\n\n")

if __name__ == "__main__":
    klue = Klue()
    data = klue.getCards()
    print(json.dumps(data, indent=2))