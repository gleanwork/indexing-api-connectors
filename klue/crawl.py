# pylint: disable-all
import gleanConstants as Constants 
import gleanApi as glean 
import gleanTools as tools
import klue as Klue

from datetime import datetime, date

CONST = Constants.Constants()
klue = Klue.Klue()

if CONST.getDebug():  
    print(f"DEBUG is: {CONST.getDebug()}")  
    print(f"VERBOSE is: {CONST.isVerbose()}")  
    print(f"BATCH_SIZE is: {CONST.BATCH_SIZE}") 
    print(f"BULK_INDEX is: {CONST.BULK_INDEX}") 
    print(f"CRAWL_TYPE: {CONST.CRAWL_TYPE}")  
    print("Using the API:", CONST.GLEAN_INDEX_API)  
    print("API Token is:", tools.maskString(CONST.GLEAN_PUSH_API_TOKEN))  
    print("Klue API Token is:", tools.maskString(CONST.KLUE_API_KEY))
else:  
    print(f"DEBUG is: {CONST.getDebug()}")  
    print(f"VERBOSE is: {CONST.isVerbose()}")  
    print(f"BATCH_SIZE is: {CONST.BATCH_SIZE}")  
print("\n\n")
 


#
# M A I N
#

#exportFile = "Extract_Cards-1722869352003.json"
#allCards = klue.getKlueCardsFromJsonFile(exportFile)
allCards = klue.getCards()

glean.configureDatasource()

#  
# Indexing Content  
if CONST.CRAWL_TYPE == "FULL" or CONST.CRAWL_TYPE == "CONTENT":  
    print("\n-=-=-= CONTENT CRAWL =-=-=-")  
   
    # To index one directly
    if CONST.DIRECT_ID != None:  
        for card in allCards:  
            if card['id'] == CONST.DIRECT_ID:  
                allCards = [ card ]  
   
    print(f"total Battle Cards: {len(allCards)}\n") 

    #if CONST.getDebug():
        #klue.printCards(allCards)

    counter = 0

    gleanDocs = []
    for card in allCards:  
        counter += 1
        doc = klue.convertKlueCardToGleanDoc(card)
        gleanDocs.append(doc)
        if CONST.BULK_INDEX == False:  
            if CONST.getDebug():  
                print(f"DEBUG: Not indexing")
            else:
                glean.indexDoc(doc)

    if CONST.BULK_INDEX == True: 
        print("Bulk Indexing")  
        uploadId = f"klue-{tools.nowString()}"
        glean.bulkIndexDocs(uploadId, gleanDocs)

print(f"Indexing Completed")  
