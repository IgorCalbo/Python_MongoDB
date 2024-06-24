from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
import urllib.parse

load_dotenv(find_dotenv())

# username = urllib.parse.quote_plus('user')
password = urllib.parse.quote_plus(os.environ.get("MONGODB_PWD"))
# password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://igorcalbo:{password}@tutorial.pweokxr.mongodb.net/?retryWrites=true&w=majority&appName=tutorial"
client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.test
collections = test_db.list_collection_names()

### INSERTING/CREATING DOCUMENTS ###
def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name": "Igor",
        "surname": "Calbo"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Igor", "Sarah", "Jennifer", "Jose", "Brad", "Allen"]
    last_names = ["Calbo", "Smith", "Bart", "Carter", "Pit", "Geral"]
    ages = [25, 40, 23, 19, 34, 67]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        document = {"First Name": first_name, "Last Name": last_name, "Age": age}
        docs.append(document)
        # person_collection.insert_one(document)
    
    person_collection.insert_many(docs)

printer = pprint.PrettyPrinter()

### READING DOCUMENTS ###
def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

def find_igor():
    igor = person_collection.find_one({"First Name": "Igor"})
    printer.pprint(igor)

def count_all_people():
    # SQL -> COUNT(*) FROM person
    # count = person_collection.find().count()
    count = person_collection.count_documents(filter={})
    print("Number of people", count)

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    # SQL -> SELECT * FROM person WHERE id = person_id
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

def get_age_range(min_age, max_age):
    # sql query
    # SELECT * FROM person WHERE age => min_age AND age <= max_age
    # gte -> greater then / equal to
    query = {"$and": [ {"Age": {"$gte": min_age} }, {"Age": {"$lte": max_age} } ]}

    people = person_collection.find(query).sort("Age")
    for person in people:
        printer.pprint(person)

def project_columns():
    columns = {"_id": 0, "First Name": 1, "Last Name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

### UPDATING DOCUMENTS ###
def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"Age": 1}, #inc -> increment 
    #     "$rename": {"First Name": "First", "Last Name": "Last"}
    # }
    # person_collection.update_one({"_id": _id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }

    person_collection.replace_one({"_id": _id}, new_doc)

replace_one("666cab038aa461d1b4c908f0")

### DELETING DOCUMENTS ###
def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})

delete_doc_by_id("666cab038aa461d1b4c908f3")


### RELATIONSHIP ###

address = {
    "_id": "62475964011a9126a4cebeb7",
    "street": "Bay Street",
    "number": 2706,
    "city": "San Francisco",
    "country": "United States",
    "zip": "94107"
}

def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.update_one({"_id": _id}, {"$addToSet": {'adresses': address}})

def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(address)

add_address_relationship("666cab038aa461d1b4c908f2", address)