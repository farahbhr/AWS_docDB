import os
import sys
import pymongo
from pymongo.errors import AutoReconnect, ConnectionFailure
import logging
import time

#Insert sample data
SEED_DATA = [
{ "_id" : 1, "name" : "Tim", "status": "active", "level": 12, "score":202},
{ "_id" : 2, "name" : "Justin", "status": "inactive", "level": 2, "score":9},
{ "_id" : 3, "name" : "Beth", "status": "active", "level": 7, "score":87},
{ "_id" : 4, "name" : "Jesse", "status": "active", "level": 3, "score":27}
]
# Insert data
def insert_data(profiles):
    try:
        profiles.insert_many(SEED_DATA)
        print("Successfully inserted data")
    except pymongo.errors.DuplicateKeyError as e:
        logging.warning(e)
    except AutoReconnect:
        print(" a connection to the database is lost and an attempt toauto-reconnect will be made")
    except ConnectionFailure :
        print("connection to the database cannot be made or is lost")
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Encountered error {e} sending evaluation results to MongoDB")
    

#Find a document
def find_data(profiles):
    query = {'name': 'Jesse'}
    print("Printing query results")
    print(profiles.find_one(query))

#Update a document
def update_data(profiles):
    print("Updating document")
    query = {'name': 'Jesse'}
    try:
        profiles.update(query, {'$set': {'level': 4}})
        print(profiles.find_one(query))
    except pymongo.errors.AutoReconnect:
        print(" a connection to the database is lost and an attempt toauto-reconnect will be made")
        time.sleep(1)
    except pymongo.errors.ConnectionFailure:
        print("connection to the database cannot be made or is lost")
    except pymongo.errors.InvalidDocument as e:
        logging.error(e)
        
   
def delete_data(profiles):
    query = {'name': 'Jesse'}
    profiles.delete_one(query)

# Clean up
def clean_data(db,client):
    db.drop_collection('profiles')
    client.close()

if __name__ == '__main__':
    # Establish DocumentDB connection
    client = pymongo.MongoClient('mongodb://imen:imen12345@docdb-cluster-demo.cluster-cxe5irpimtve.us-east-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
    db = client.sample_database
    profiles = db['profiles']
    logging.basicConfig(
        filename='app.log',
        level=logging.ERROR,
        format=f'%(asctime)s %(levelname)s %(message)s'
    )

    logger = logging.getLogger()
    logger.debug('The script is starting.')
    insert_data(profiles)
    find_data(profiles)
    update_data(profiles)
    delete_data(profiles)
    clean_data(profiles)


