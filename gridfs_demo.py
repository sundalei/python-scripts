import pymongo
import gridfs

client = pymongo.MongoClient('mongodb+srv://analytics:analytics-password@analytics.zp8ia.mongodb.net/mflix?retryWrites=true&w=majority')
db = client.test
fs = gridfs.GridFS(db)
with open('../neighborhoods.json', 'rb') as neighborhoods:
    data = neighborhoods.read()
    fs.put(data, filename='neighborhoods.json', foo='bar')
with open('../restaurants.json', 'rb') as restaurants:
    data = restaurants.read()
    fs.put(data, filename='restaurants.json', foo='bar')