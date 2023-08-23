import json
import pymongo

#Open the files with the received messages
f = open("people.txt", "r")
peopleJsons = []
peopleTxt = f.readlines()
for line in peopleTxt:
    peopleJsons.append(json.loads(line))

f = open("products.txt", "r")
productsJsons = []
productsTxt = f.readlines()
for line in productsTxt:
    productsJsons.append(json.loads(line))

#Initialize the mongoDB connector
client = pymongo.MongoClient("mongodb://localhost:27017")
database = client["fusedData"]
collection = database["users"]
collection.delete_many({})

#Proceed to Data Fusion
productsDetails = []
for personJson in peopleJsons:
    personJson["productsBought"] = []
    for id in personJson["product_ids"]:
        for productJson in productsJsons:
            if str(id) == str(productJson["id"]):
                productsDetails.append(productJson)
    personJson["productsBought"] = productsDetails
    productsDetails = []
    collection.insert_one(personJson)