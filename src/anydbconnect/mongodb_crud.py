from typing import Any, Union
import pandas as pd
from pymongo.mongo_client import MongoClient
import json
from ensure import ensure_annotations
import json 

class mongo_operation:
    """This is MongoDB connector class which allows CRUD operation 
    
    Keyword arguments:
    client_url -- MongoDB Connection String
    database_name -- MongoDB Database Name
    collection_name -- Database's Collection Name
    """

    def __init__(self, client_url: str, database_name: str, collection_name: str):
        self.client_url = client_url
        self.database_name = database_name
        self.collection_name = collection_name

    def create_client(self):
        """Client Connection to MongoDB server"""
        client = MongoClient(self.client_url)
        return client

    def create_database(self):
        """Creates DataBase
        """
        
        client = self.create_client()
        database = client[self.database_name]
        return database
        
    def create_collection(self,collection:str):
        """Creates Collection"""
        database = self.create_database()
        collection = database[collection]
        return collection

    def insert_record(self,record:Union[dict,list],collection_name:str) -> Any: 
        """Insert Single/ Multiple record in Provided Collection 
        
        Keyword arguments:
        record -- record to insert (dict / list of dict)
        collection_name -- name of collection

        """
        
        if type(record) == list:
            for data in record:
                if type(data) != dict:
                    raise TypeError("record must be in the dict")
            collection = self.create_collection(collection_name)
            collection.insert_many(record)
        elif type(record) == dict:
            collection = self.create_collection(collection_name)
            collection.insert_one(record)

    def bulk_insert(self,datafile:str,collection_name:str):
        """Insert Bulk record (csv, xlsx) in Provided Collection 
        
        Keyword arguments:
        datafile -- File Path (.csv / .xlsx)
        collection_name -- name of collection

        """

        self.path = datafile

        if self.path.endswith('.csv'):
            data = pd.read_csv(self.path,encoding='utf-8')

        elif self.path.endswith('.xlsx'):
            data = pd.read_excel(self.path,encoding='utf-8')

        else:
            return "Please Provide csv/ xlsx path only"
            
        datajson = json.loads(data.to_json(orient='record'))
        collection = self.create_collection(collection_name)
        collection.insert_many(datajson)
