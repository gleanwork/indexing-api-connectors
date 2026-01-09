import requests, json, base64
import gleanConstants as Constants
import gleantools as tools

CONST = Constants.Constants()


class Slab:
   """A Class for interacting with Slab"""

   # The Slab API endpoint to use
   apiURL = None

   # Server API Token
   token = None

   # Built header for auth
   headers = None

   # topics cache for id resolution
   topics = None

   def __init__(self) -> None:

      self.token = CONST.SLAB_TOKEN
      self.apiURL = CONST.SLAB_API_URL

      self.setAuthHeader()

      self.getTopicsIndex()

      if CONST.getDebug() or CONST.isVerbose():
         print("Slab token:", self.token)
         print("Headers:", self.headers)


   def setAuthHeader(self):
      """
      https://the.slab.com/public/posts/xxxxxxxx#organization
      """
      graphQLEndpoint =  self.apiURL + "/v1/graphql"
      getHostQuery =  { 
         "query": "query { organization { host } }"
      }

      self.headers = {
         "accept": "application/json",
         "authorization": self.token
      }

      response = requests.post(graphQLEndpoint, data=getHostQuery, headers=self.headers, verify=True)

      try:
         decodedResponse = json.loads(response.text)
         tokenHost = decodedResponse['data']['organization']['host'] 
         self.apiURL = "https://" + tokenHost + "/v1/graphql"
         print("Host Response", decodedResponse)
         print("Updating API Host:", self.apiURL)

      except:
         print("Error in getting host data from API - problem with auth token?")
         exit()



   def getContentAndAuthors(self, postContent):

      decodedContent = json.loads(postContent)
      numParts = len(decodedContent)
      partCount = 0
      contentText = ""

      authors = []

      # Go through each content piece and collect the text and authors
      if numParts > 0:
         for part in decodedContent:
            partCount += 1

            try:
               contentText += part['insert']
            except:
               pass

            try:
               author = part['attributes']['author']
               if author not in authors:
                  authors.append(author)

            except:
               pass

            #print(f"part {partCount}: {part}")
         #print(f"Text: {contentText}")

      return contentText, authors



   def getAllPosts(self):
      getPostsQuery =  { 
         "query": "query { organization { posts { archivedAt, id, title } } }" 
      }

      response = requests.post(self.apiURL, data=getPostsQuery, headers=self.headers, verify=True)

      try:
         decodedResponse = json.loads(response.text)
         posts = decodedResponse['data']['organization']['posts'] 
         numPosts = len(posts)

         if CONST.isVerbose():
            print(f"Retrieved {numPosts} posts from API")

      except:
         print(f"Error in getting posts data from API - Response: {response.text}")
         exit()

      return posts


   def getPostDetails(self, post):

      getPostDetailsQuery =  { 
         "query": "query { post(id: \"" + post['id'] + "\") { archivedAt, content, id, insertedAt, linkAccess, owner { email }, publishedAt, title, topics { id }, updatedAt } }" 
      }

      response = requests.post(self.apiURL, data=getPostDetailsQuery, headers=self.headers, verify=True)

      try:
         decodedResponse = json.loads(response.text)
         post = decodedResponse['data']['post'] 
         if CONST.isVerbose():
            print(f"Retrieved {post['id']} post from API")

      except:
         print(f"Error in getting posts data from API - Response: {response.text}")
         exit()

      content, authors = self.getContentAndAuthors(post['content'])

      filteredPost = {}
      firstTopic = ""
      container = ""

      try:
         #print(f"topics - {post['topics']}")
         firstTopic = post['topics'][0]['id']
         container = self.resolveTopicContainer(firstTopic)
      except: pass

      filteredPost['author'] = post['owner']['email']
      filteredPost['authors'] = authors
      filteredPost['body'] = { "mime_type": "text/plain", "text_content": content }
      filteredPost['container'] = container
      filteredPost['created_at'] = tools.dateToEpoch(post['publishedAt'])
      filteredPost['id'] = post['id']
      filteredPost['private'] = self.isTopicPrivate(firstTopic)
      filteredPost['title'] = post['title']
      filteredPost['topic'] = self.getTopicNameFromId(firstTopic)
      filteredPost['updated_at'] = tools.dateToEpoch(post['updatedAt'])
      filteredPost['updated_by'] = post['owner']['email']
      filteredPost['view_url'] = CONST.DATASOURCE_VIEWURLBASE + "posts/" + post['id']

      return filteredPost


   def getTopics(self):

      getTopicsQuery =  { 
         "query": "query { organization { topics { id, name, description, owners { id, email }, hierarchy, privacy, members { id, email}, memberEditable, ownerGroups { id, name }, memberGroups { id, name } } } }"
      }

      response = requests.post(self.apiURL, data=getTopicsQuery, headers=self.headers, verify=True)

      try:
         decodedResponse = json.loads(response.text)
         topics = decodedResponse['data']['organization']['topics'] 
         numTopics = len(topics)
         if CONST.isVerbose():
            print(f"Retrieved {numTopics} topics from API")

      except:
         print(f"Error in getting topics data from API - Response: {response.text}")
         exit()

      return topics


   def getTopicNameFromId(self, id):
      name = "Sys"
      try:
         name = self.topics[id]['name']
      except: pass
      return name


   def isTopicPrivate(self, id):
      try:
         if self.topics[id]['privacy'] == "PRIVATE":
            return True
      except:
         return False


   def resolveTopicContainer(self, id):
      container = "Sys"
      try:
         topic = self.topics[id]
         counter = 0
         #print(f"topic: {topic['id']} - hierarchy: {topic['hierarchy'][0]}")
         for section in topic['hierarchy'][0].split('.'):
            name = self.getTopicNameFromId(section)
            if counter == 0:
               container = name
            else:
               container += " / " + name
            #print(f"section: {section} - container {container}")
            counter += 1
      except: pass
      return container


   def getTopicsIndex(self):
      topicsIndex = {}
      topics = self.getTopics()

      for topic in topics:
         topicsIndex[topic['id']] = topic

      self.topics = topicsIndex

      if CONST.isVerbose() and CONST.getDebug():
         # test the index
         counter = 0
         for topic in topics:
            id = topic['id']
            counter += 1
            print(f"topic {counter} - id: {topic['id']} - name: {self.getTopicNameFromId(id)}")
         print("\n")


   def getUsers(self):

      getHostQuery =  { 
         "query": "query { organization { users(includeDeactivated: true) { id, name, email, deactivatedAt } } }"
      }

      response = requests.post(self.apiURL, data=getHostQuery, headers=self.headers, verify=True)

      try:
         decodedResponse = json.loads(response.text)
         users = decodedResponse['data']['organization']['users'] 
         numUsers = len(users)
         print(f"Retrieved {numUsers} users from API")

      except:
         print(f"Error in getting users data from API - Response: {response.text}")
         exit()

      return users