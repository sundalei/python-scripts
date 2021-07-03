from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.errors import ConnectionFailure, OperationFailure

uri = 'mongodb+srv://analytics:analytics-password@analytics.zp8ia.mongodb.net'
client = MongoClient(uri)
my_wc_majority = WriteConcern('majority', wtimeout=1000)

client.get_database('webshopcallback2', write_concern=my_wc_majority).orders.insert_one({'sku': 'abc123', 'qty': 0})
client.get_database('webshopcallback2', write_concern=my_wc_majority).inventory.insert_one({'sku': 'abc123', 'qty': 1000})


def callback(my_session):
    orders = my_session.client.webshopcallback2.orders
    inventory = my_session.client.webshopcallback2.inventory

    orders.insert_one({'sku': 'abc123', 'qty': 100}, session=my_session)
    inventory.update_one({'sku': 'abc123', 'qty': {'$gte': 100}}, {'$inc': {'qty': -100}}, session=my_session)


with client.start_session() as session:
    session.with_transaction(callback, read_concern=ReadConcern('local'), write_concern=my_wc_majority, read_preference=ReadPreference.PRIMARY)