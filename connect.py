import pymongo
import os
import datetime

#Connection details:
uri = "mongodb+srv://DETAILS.mongodb.net/?" +\
"authSource=%24external&" +\
"authMechanism=MONGODB-X509&" +\
"retryWrites=true&" +\
"w=majority"

#Connect to MongoDB
client = pymongo.MongoClient(uri,
tls=True,
tlsCertificateKeyFile="PATH_TO_KEY.pem")

db = client["B30DM"]
collection = db["testCol"]
doc_count = collection.count_documents({})
print(doc_count)

'''
post = {"author":
{
"user":"emm24",
"fullname":"Eve McAleer"
},
"text": "Test_EM",
"tags": ["mongodb", "python", "pymongo"],
"date": datetime.datetime.utcnow()}
posts = db.posts
posts.insert_one(post)
'''

coke = {
"code":"coke",
"name":"Coca-cola",
"provider":"cocacolaco",
"wholesale_price":0.85,
"sale_price":1.5
}
products = db.products
products.insert_one(coke)
