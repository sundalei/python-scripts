from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.errors import ConnectionFailure, OperationFailure

uri = 'mongodb+srv://analytics:analytics-password@analytics.zp8ia.mongodb.net'
client = MongoClient(uri)
my_wc_majority = WriteConcern('majority', wtimeout=1000)

client.get_database('webshop', write_concern=my_wc_majority).orders.insert_one({'sku': 'abc123', 'qty': 0})
client.get_database('webshop', write_concern=my_wc_majority).inventory.insert_one({'sku': 'abc123', 'qty': 1000})


def update_orders_and_inventory(session):
    orders = session.client.webshop.orders
    inventory = session.client.webshop.inventory

    with session.start_transaction(read_concern=ReadConcern('snapshot'),write_concern=WriteConcern(w='majority'), read_preference=ReadPreference.PRIMARY):
        orders.insert_one({'sku': 'abc123', 'qty': 100}, session=session)
        inventory.update_one({'sku': 'abc123', 'qty': {'$gte': 100}}, {'$inc': {'qty': -100}}, session=session)

        commit_with_retry(session)


def commit_with_retry(session):
    while True:
        try:
            session.commit_transaction()
            print('Transaction committed.')
            break
        except (ConnectionFailure, OperationFailure) as exc:
            if exc.has_error_label('UnknownTransactionCommitResult'):
                print("UnknownTransactionCommitResult, retrying commit operation ...")
                continue
            else:
                print("Error during commit ...")
                raise



def run_transaction_with_retry(txn_func, session):
    while True:
        try:
            txn_func(session)
            break
        except (ConnectionFailure, OperationFailure) as exc:
            if exc.has_error_label("TransientTransactionError"):
                print("TransientTransactionError, retrying transaction ...")
                continue
            else:
                raise



with client.start_session() as my_session:
    try:
        run_transaction_with_retry(update_orders_and_inventory, my_session)
    except Exception as exc:
        raise

print(client)