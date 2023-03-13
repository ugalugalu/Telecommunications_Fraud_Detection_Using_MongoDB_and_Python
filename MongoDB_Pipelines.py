from pprint import pprint
import pandas as pd
import pymongo
import logging
from pymongo import UpdateOne,DeleteOne
from pymongo.errors import BulkWriteError


# Extraction function
def extract_data():
    # Load call log data from CSV file
    call_logs = pd.read_csv('call_logs.csv')

    # Convert call duration to minutes for easier analysis
    call_logs['duration_minutes'] = call_logs['call_duration'] / 60

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data extraction completed.")

    return call_logs

# Transformation function
def transform_data(call_logs):
    # Data cleaning and handling missing values
    transformed_data=call_logs.dropna()
    transform_data = call_logs.drop_duplicates()

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data transformation completed.")
    transformed_data= transformed_data.to_dict('records')
    return transformed_data

# Loading function
def load_data(transformed_data):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://mongo:mongo@cluster0.yj2pr.mongodb.net/minPoolSize=5&maxPoolSize=10?retryWrites=true&w=majority",ssl=True,tlsInsecure=True)
    db = client["galugalu"]
    collection = db["galugalu"]

    

    # Create indexes on the collection and compress data using snappy algorithm
    collection.create_index([('call_duration',pymongo.DESCENDING)],
                           storageEngine = {
                           'wiredTiger': {
                              'configString': 'block_compressor=snappy'
                           }
                        } 
                    )

    # Use bulk inserts to optimize performance
 
    collection.insert_many(transformed_data)

    #Demonstrate the ability to execute mixed bulk write operations, will be combining one update and Delete operations

    requests = [
        UpdateOne({"call_id":1},{'$set':{'call_type':'Incoming'}}),
        DeleteOne({'call_id':2})
    ]
    try:
        collection.bulk_write(requests)
    except BulkWriteError as bwe:
       pprint(bwe.details)
    

    # Use Python logging module to log errors and activities
    logger = logging.getLogger(__name__)
    logger.info("Data loading completed.")

if __name__ == '__main__':
    
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)