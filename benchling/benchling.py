import requests, json, base64
import gleanConstants as Constants
import gleantools as tools
import concurrent.futures

from datetime import datetime
from benchling_sdk.benchling import Benchling as BenchlingSDK
from benchling_sdk.auth.api_key_auth import ApiKeyAuth
from benchling_sdk.auth.client_credentials_oauth2 import ClientCredentialsOAuth2
from benchling_sdk.models import Entry


CONST = Constants.Constants()


class Benchling:
   """A Class for interacting with Benchling"""

   # The Benchling API endpoint to use
   apiURL = None

   # Server API Token key can be generated with clientId and clientSecret
   key = None
   secret = None

   # SDK Instance
   sdk = None

   # include username prefixed with '='
   user = None

   authToken = None
   anonToken = None

   headers = None
   anonHeaders = None

   def __init__(self) -> None:

      self.key = CONST.BENCHLING_KEY
      self.secret = CONST.BENCHLING_SECRET
      self.user = "=" + CONST.BENCHLING_ADMIN_USER
      self.apiURL = CONST.BENCHLING_URL

      self.refreshSDK()

      self.generateToken()

      if CONST.getDebug() or CONST.isVerbose():
         print("Benchling key:", self.key)
         print("Benchling secret:", self.secret)
         print("Benchling Admin:", self.user)
         print("Headers:", self.headers)


   def refreshSDK(self):
      auth_method = ClientCredentialsOAuth2(
         client_id = self.key,
         client_secret = self.secret
      )

      self.sdk = BenchlingSDK(
         url = self.apiURL,
         auth_method = auth_method
      )


   def generateToken(self):
      tokenURL = CONST.BENCHLING_URL + "/api/v2/token"

      authorizationString = self.key + ":" + self.secret
      authorizationBase64 = base64.b64encode(authorizationString.encode("ascii")).decode("ascii")

      headers = {
         "accept": "application/x-www-form-urlencoded, application/json",
         "authorization": "Basic " + authorizationBase64
      }

      params = {
         "grant_type": "client_credentials"
      }

      response = requests.post(tokenURL, data=params, headers=headers, verify=True)

      try:
         decodedResponse = json.loads(response.text)
         self.authToken = decodedResponse['access_token']

         self.headers = {
            "authorization": "Bearer " + self.authToken
         }

      except:
         print("Error in generating refresh token!")
         exit()


   def getEntries(self):
      """
      https://benchling.com/api/reference#/Entries

      Discover API errors or transform response data here
      """

      # Refresh API token
      self.generateToken()

      allEntries = []

      pageSize = 10
      if CONST.MAX_IDX < pageSize:
         pageSize = CONST.MAX_IDX

      #pages = self.sdk.entries.list_entries(ids=["etr_h6g28wTn"])
      pages = self.sdk.entries.list_entries(page_size = pageSize)

      #numPages = len(pages)
      print(f"starting entry export")

      pagecount = 0
      totalEntries = 0

      # The SDK uses an iterator which is an unknown length. Using a breaker
      # to exit out in a sane amount of time.
      breaker = False
      for page in pages:
         if breaker: break
         pagecount += 1

         numEntries = len(page)
         print(f"\nentries - page: {pagecount} - entries in page: {numEntries}")


         entrycount = 0
         for entry in page:
            entrycount += 1
            totalEntries += 1

            print(f"entries - page: {pagecount} - page entry: {entrycount} - entry.id: {entry.id} - current total: {totalEntries}")

            daynotes = ""
            for day in entry.days:
               for note in day.notes:
                  try: 
                     if (len(note.text) > 0):
                        daynotes += note.text + "\n"
                  except:
                     pass

            filteredEntry = {}
            filteredEntry['id'] = entry.id
            filteredEntry['created_at'] = int(entry.created_at.timestamp())
            filteredEntry['author'] = entry.creator.id
            filteredEntry['title'] = entry.name
            # the modified_at format = 2022-12-01T01:45:40.001530+00:00
            # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
            filteredEntry['updated_at'] = int(datetime.strptime(entry.modified_at,"%Y-%m-%dT%H:%M:%S.%f%z").timestamp())
            try:
               filteredEntry['updated_by'] = entry.authors[0].id
            except:
               filteredEntry['updated_by'] = entry.creator.id
            filteredEntry['body'] = { "mime_type": "text/plain", "text_content": daynotes }
            filteredEntry['container'] = entry.folder_id
            filteredEntry['view_url'] = entry.web_url
   
            #print("filteredEntry", filteredEntry)
            allEntries.append(filteredEntry)

            if totalEntries >= CONST.MAX_IDX:
               breaker = True
               break


      return allEntries

   def getFolders(self):
      """
      https://benchling.com/api/reference#/Folders

      Discover API errors or transform response data here
      """

      # Refresh API token
      self.generateToken() 

      allFolders = []

      pages = self.sdk.folders.list()

      totalFolders = 0
      pagecount = 0
      for page in pages:
         pagecount += 1

         numFolders = len(page)
         # print(f"\nfolders - page: {pagecount} - folders: {numFolders}")

         foldercount = 0
         for folder in page:
            foldercount += 1
            totalFolders += 1

            # print(f"projects - page: {pagecount} - folder: {foldercount}")

            allFolders.append(folder)

      return allFolders



   def getProjects(self):
      """
      https://benchling.com/api/reference#/Projects

      Discover API errors or transform response data here
      """

      # Refresh API token
      self.generateToken() 

      allProjects = []

      pages = self.sdk.projects.list()

      totalProjects = 0
      pagecount = 0
      for page in pages:
         pagecount += 1

         numProjects = len(page)
         # print(f"\nprojects - page: {pagecount} - projects: {numProjects}")

         projectcount = 0
         for project in page:
            projectcount += 1
            totalProjects += 1

            # print(f"projects - page: {pagecount} - entry: {projectcount}")

            allProjects.append(project)

      return allProjects


   def getUsers(self):

      # Refresh API token
      self.generateToken()

      allUsers = []

      # user User(email='****@****.com',
      #           is_suspended=False,
      #           password_last_changed_at='2022-02-28T14:53:21.391936+00:00',
      #           handle='****',
      #           id='ent_qxdmfed2',
      #           name='Bob Robertson',
      #           additional_properties={})
      users = self.sdk.users.list()

      pagecount = 0
      for page in users:
         pagecount += 1
         usercount = 0
         for user in page:
            usercount += 1
            allUsers.append(user)

      return allUsers









