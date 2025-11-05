# pylint: disable-all
#fetch coupa data
import requests, traceback
import gleanConstants as Constants
import gleantools as tools
from indexer import configureDatasource, bulkIndexDocs, indexDocs
from concurrent.futures import ThreadPoolExecutor, as_completed
from glean_indexing_api_client.model.custom_property import CustomProperty


CONST = Constants.Constants()
NUM_THREADS = 5
CHUNK_SIZE = 50


class Coupa:

	base_url = CONST.COUPA_BASE_URL + "/api"
	headers = {}
	
	def __init__(self):
		self.headers = {
			"accept": "application/json",
			"content-type": "application/x-www-form-urlencoded",
			"Authorization": "Bearer " + self.generate_token()
			}

	def generate_token(self):
		print("generating access token....")
		url = CONST.COUPA_BASE_URL + "/oauth2/token"

		payload = {
			"grant_type": "client_credentials",
			"client_id": CONST.COUPA_CLIENT_ID,
			"client_secret": CONST.COUPA_CLIENT_SECRET,
			"scope": CONST.COUPA_SCOPES
		}

		headers = {
				"Accept": "application/json",
				"Content-Type": "application/x-www-form-urlencoded"
		}

		response = requests.post(url, data=payload, headers=headers)
		access_token = response.json()['access_token']
		return access_token

	#fetches list of active suppliers
	def get_suppliers(self):
		OFFSET = 0
		object_type = "supplier"
		fields = '["id","created-at","updated-at","name","display-name","number","tax-id", \
		"status",{"updated_by":["id","email","fullname"]},"invoice-matching-level", \
		{"payment_term":["code"]},{"created_by":["id","email","fullname"]},{"supplier_addresses":["city","state",{"country":["name"]}]}]'
		
		while True:
			url = f"{self.base_url}/suppliers?offset={str(OFFSET)}&status=active&fields={fields}"
			response = requests.get(url,headers=self.headers)
			data = response.json()
			
			for row in data: 
				doc = {}
				try:
					body = f"{row['number']} {row['tax-id']} {row['name']} {row['display-name']}"
					doc['id'] = str(row['id'])
					doc['title'] = row['display-name']
					doc['datasource'] = CONST.DATASOURCE_NAME
					doc['objectType'] = object_type
					doc['body'] = {"mime_type":"text/plain", "textContent":body}
					doc['status'] = row['status']
					doc['summary'] = doc['body']
					doc['author'] = {"email": row['created-by']['email'], "datasourceUserId": row['created-by']['id'], "name": row['created-by']['fullname'] }
					doc['updatedBy'] = {"email": row['updated-by']['email'], "datasourceUserId": row['updated-by']['id'], "name": row['updated-by']['fullname'] }
					doc['createdAt'] = tools.convert_date(row['created-at'])
					doc['updatedAt'] = tools.convert_date(row['updated-at'])
					doc["permissions"] = {"allowAnonymousAccess": True}
					doc['viewURL'] = f"{CONST.DATASOURCE_VIEWURLBASE}/suppliers/{doc['id']}/record"

					addresses = row['supplier-addresses'][0]
					#print(f"supplier-addresses: {row['supplier-addresses']}")

					if addresses['country'] and 'name' in addresses['country']:
						doc['custom'] = []

						if addresses['country']['name']:
							doc['custom'].append(
								CustomProperty(
									name = "suppliercountry",
									value = str(addresses['country']['name'])
								))


						if addresses['city']:
							doc['custom'].append(
								CustomProperty(
									name = "suppliercity",
									value = str(addresses['city'])
								))
							
						if addresses['state']:
							doc['custom'].append(
								CustomProperty(
									name = "supplierstate",
									value = str(addresses['state'])
								))

					indexDocs(doc)
				except Exception as e:
					print(f"error: {e}")
					traceback.print_exc()
					continue

			if len(data) < CHUNK_SIZE:
				break
			else:
				OFFSET += CHUNK_SIZE

	
	def get_legalEntities(self):
		OFFSET = 0
		object_type = "legalentity"
		fields = '["id","created-at","updated-at","name","abbreviation",{"legal_entity_address":["street1","city","city",{"country":["name"]}]}]'

		while True:
			url = f"{self.base_url}/legal_entities?offset={str(OFFSET)}&active=true&fields={fields}"
			response = requests.get(url,headers=self.headers)
			data = response.json()
			for row in data:
				doc = {}
				try:
					doc['id'] = str(row['id'])
					doc['title'] = row['name']
					doc['datasource'] = CONST.DATASOURCE_NAME
					doc['objectType'] = object_type
					doc['viewURL'] = CONST.DATASOURCE_VIEWURLBASE + "/legal_entities/" + str(row['id'])
					body = f"{row['name']}\n{row['abbreviation']}\n{row['legal-entity-address']['street1']}\n{row['legal-entity-address']['city']}"
					body = f"{body}\n{row['legal-entity-address']['country']['name']}"
					doc['body'] = {"mime_type":"text/html", "textContent":body}
					doc['summary'] = doc['body']
					doc['status'] = "active"
					doc['createdAt'] = tools.convert_date(row['created-at'])
					doc['updatedAt'] = tools.convert_date(row['updated-at'])
					doc["permissions"] = {"allowAnonymousAccess": True}

					indexDocs(doc)
				except Exception as e:
					print(f"error: {e}")
					continue

			if len(data) < CHUNK_SIZE:
				break
			else:
				OFFSET += CHUNK_SIZE
	

	def get_contracts(self):
		OFFSET = 0
		object_type = "contract"
		fields = '["id","created-at","description","updated-at","name","type","start-date","end-date","status", \
		{"updated_by":["id","email","fullname"]},{"created_by":["id","email","fullname"]}]'

		while True:
			url = f"{self.base_url}/contracts?offset={str(OFFSET)}&fields={fields}"
			response = requests.get(url,headers=self.headers)
			data = response.json()
			for row in data:
				doc = {}
				try:
					doc['id'] = str(row['id'])
					doc['title'] = row['name']
					doc['datasource'] = CONST.DATASOURCE_NAME
					doc['objectType'] = object_type
					doc['viewURL'] = CONST.DATASOURCE_VIEWURLBASE + "/contracts/" + str(row['id'])
					body = f"{row['name']}\n{row['start-date']}\n{row['end-date']}\n{row['description']}"
					doc['body'] = {"mime_type":"text/html", "textContent":body}
					doc['summary'] = doc['body']
					doc['status'] = row['status']
					doc['createdAt'] = tools.convert_date(row['created-at'])
					doc['updatedAt'] = tools.convert_date(row['updated-at'])
					doc['author'] = {"email": row['created-by']['email'], "datasourceUserId": row['created-by']['id'], "name": row['created-by']['fullname'] }
					doc['updatedBy'] = {"email": row['updated-by']['email'], "datasourceUserId": row['updated-by']['id'], "name": row['updated-by']['fullname'] }
					doc["permissions"] = {"allowAnonymousAccess": True}

					indexDocs(doc)
				except Exception as e:
					print(f"error: {e}")
					continue

			if len(data) < CHUNK_SIZE:
				break
			else:
				OFFSET += CHUNK_SIZE

			
coupa = Coupa()
coupa.get_suppliers()
coupa.get_legalEntities()
coupa.get_contracts()