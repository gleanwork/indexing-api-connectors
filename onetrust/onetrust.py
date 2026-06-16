import requests
import gleanConstants as Constants
import gleantools as tools
from datetime import datetime
import time
from indexer import configureDatasource, indexDocs


CONST = Constants.Constants()

class Onetrust:
    client_id = None
    client_secret = None
    def __init__(self) -> None:
        self.client_id = CONST.ONETRUST_CLIENT_ID
        self.client_secret = CONST.ONETRUST_CLIENT_SECRET
        self.apiURL = CONST.ONETRUST_BASE_URL

        if CONST.getDebug() or CONST.isVerbose():
            print("Onetrust client id:", self.client_id)
            print("Onetrust client secret:", self.client_secret)

    def generateToken(self):
        url = self.apiURL + "/api/access/v1/oauth/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {
                "accept": "application/json",
                "content-type": "application/x-www-form-urlencoded"
        }
        response = requests.post(url, data=payload, headers=headers)
        accessTokenJson=response.json()
        accessToken=accessTokenJson["access_token"]
        #print(accessTokenJson)
        #print(accessToken)
        return accessToken

    
    def getAssessmentList(self):
        AssessmentList = []
        pageSize = 100
        counter = 1
        token = self.generateToken()
        url = self.apiURL + "/api/assessment/v2/assessments?page=0&size=" + str(pageSize)
        headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": "Bearer " + token
        }
        response = requests.get(url, headers=headers)
        assessmentSample = response.json()
        #print(assessmentSample)
        totalAssessments = assessmentSample["page"]["totalElements"]
        totalPages = assessmentSample["page"]["totalPages"]
        print("TotalAssessment: " + str(totalAssessments))
        print("TotalPagesOfAssessment: " + str(totalPages))
        TokenGenerationTime = datetime.now()

        for i in range(totalPages):
            TimeNow = datetime.now()
            if abs(int((TokenGenerationTime-TimeNow).total_seconds()))/60 >= 30:
                time.sleep(60)
                token = self.generateToken()
                headers["Authorization"] = "Bearer " + token
                TokenGenerationTime = datetime.now()
                print("NewTokenGenerated: " + str(TokenGenerationTime))

            url = self.apiURL + "/api/assessment/v2/assessments?page=" + str(i) + "&size=" + str(pageSize)
            response = requests.get(url, headers=headers)
            try:
                AssessmentListPage = response.json()
            except:
                print("not able to get the list for page: " + str(i))
                continue

            for assessment in AssessmentListPage["content"]:
                #print("Processing Assessment with id: " + str(assessment["assessmentId"]))
                assessmentInfo = {}
                try:
                    CreatorandRiskLevelInfo = self.getAssessmentCreatorAndRisk(token, assessment["assessmentId"])
                    assessmentInfo["number"] = assessment["number"]
                    assessmentInfo["assessmentId"] = assessment["assessmentId"]
                    assessmentInfo["status"] = assessment["status"]
                    assessmentInfo["name"] = assessment["name"]
                    assessmentInfo["creator"] = {"name": CreatorandRiskLevelInfo["creator"], "datasourceUserId": CreatorandRiskLevelInfo["creatorId"]}
                    Summary = assessmentInfo["name"] + "-" + assessmentInfo["assessmentId"] + "-" + str(assessmentInfo["number"]) + "-" + CreatorandRiskLevelInfo["creator"] + "-" + assessmentInfo["status"]
                    assessmentInfo["summary"] = {"mime_type":"text/plain", "textContent": Summary}
                    #{"mime_type":"text/plain", "textContent":page["Summary"]}
                    assessmentInfo["body"] = {"mime_type":"text/plain", "textContent": Summary}
                    assessmentInfo["lowRiskCount"] = CreatorandRiskLevelInfo["lowRiskCount"]
                    assessmentInfo["mediumRiskCount"] = CreatorandRiskLevelInfo["mediumRiskCount"]
                    assessmentInfo["highRiskCount"] = CreatorandRiskLevelInfo["highRiskCount"]
                    assessmentInfo["veryHighRiskCount"] = CreatorandRiskLevelInfo["veryHighRiskCount"]
                    assessmentInfo["residualRiskScore"] = CreatorandRiskLevelInfo["residualRiskScore"]
                    assessmentInfo["updatedAt"] = int(datetime.strptime(assessment["lastUpdated"],"%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
                    #int(datetime.strptime(page["DateUpdated"],"%Y-%m-%dT%H:%M:%SZ").timestamp())
                    assessmentInfo["viewURL"] = self.apiURL + "/vendor/assessments/details/" + assessment["assessmentId"] + "?type=assessments"
                    assessmentInfo["permissions"] = {"allowAnonymousAccess": True}
                    assessmentInfo["datasource"] = CONST.DATASOURCE_NAME
                    print("Appending Assessment " + str(counter) + " with id: " + str(assessment["assessmentId"]))
                    AssessmentList.append(assessmentInfo)
                    counter = counter + 1
                except:
                    print("Assessment with Id: " + assessment["assessmentId"] + " missed. Moving ahead")
                    continue
        tools.writeToCSV(AssessmentList, "AssessmentList.csv")
        

    def getAssessmentCreatorAndRisk(self, token, assessmentId):
        assessmentCreatorAndRiskInfo = {}
        url = self.apiURL + "/api/assessment/v2/assessments/" + str(assessmentId) + "/export"
        headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": "Bearer " + token
        }
        response = requests.get(url, headers=headers)
        #print(response)
        assessmentDetails = response.json()
        assessmentCreatorAndRiskInfo["creator"] = assessmentDetails["createdBy"]["name"]
        assessmentCreatorAndRiskInfo["creatorId"] = assessmentDetails["createdBy"]["id"]
        assessmentCreatorAndRiskInfo["lowRiskCount"] = assessmentDetails["lowRisk"]
        assessmentCreatorAndRiskInfo["mediumRiskCount"] = assessmentDetails["mediumRisk"]
        assessmentCreatorAndRiskInfo["highRiskCount"] = assessmentDetails["highRisk"]
        assessmentCreatorAndRiskInfo["veryHighRiskCount"] = assessmentDetails["veryHighRisk"]
        assessmentCreatorAndRiskInfo["residualRiskScore"] = assessmentDetails["residualRiskScore"]

        return assessmentCreatorAndRiskInfo



onetrust = Onetrust()
configureDatasource()
onetrust.getAssessmentList()

if CONST.BULK_INDEX:
    print("BULK_INDEX is set but bulk indexing is not implemented; "
          "falling back to single-document indexing.")
indexDocs("AssessmentList.csv")
