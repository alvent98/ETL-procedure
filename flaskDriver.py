import flask
import pymongo

app = flask.Flask(__name__)

@app.route("/<int:personId>/")
def get_user_products(personId):
    client = pymongo.MongoClient("mongodb://localhost:27017")
    database = client["fusedData"]
    collection = database["users"]
    person = collection.find_one({"id" : personId})
    return str(person["productsBought"])