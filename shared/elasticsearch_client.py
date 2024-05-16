from elasticsearch import Elasticsearch
import time

class ElasticsearchClient:
    def __init__(self, host="localhost", port="9200"):
        self.host = host
        self.port = port
        self.client = Elasticsearch(["http://"+self.host+":"+self.port])
        self.create_index('sensors')
        mapping = {
            'properties': {
                'name': {'type': 'keyword'},
                'latitude': {'type': 'float'},
                'longitude': {'type': 'float'},
                'type': {'type': 'text'},
                'mac_address': {'type': 'text'},
                'manufacturer': {'type': 'text'},
                'model': {'type': 'text'},
                'serie_number': {'type': 'text'},
                'firmware_version': {'type': 'text'},
                'description': {'type': 'text'},
            }    
        }
        # Action plsssss
        self.create_mapping('sensors',mapping=mapping)
        # Active waiting
        while not self.ping():
            print("Waiting for Elasticsearch to start...")
            time.sleep(1)        

    def ping(self):
        return self.client.ping()
    
    def clearIndex(self, index_name):
        if self.client.indices.exists(index=index_name):
            # If the index exists, delete it
            return self.client.indices.delete(index=index_name)
        else:
            # If the index does not exist, do nothing
            return None
    
    def close(self):
        self.client.close()

    def create_index(self, index_name):
        if not self.client.indices.exists(index=index_name):
            return self.client.indices.create(index=index_name)
        else:
            print(f"Index '{index_name}' already exists.")
    
    def create_mapping(self, index_name, mapping):
        if not self.client.indices.exists(index=index_name):
            return None  # Index doesn't exist
        current_mapping = self.client.indices.get_mapping(index=index_name)
        if current_mapping == mapping:
            return None  # Mapping already matches
        return self.client.indices.put_mapping(index=index_name, body=mapping)
    
    def search(self, index_name, query):
        return self.client.search(index=index_name, body=query)
    
    def index_document(self, index_name, document):
        return self.client.index(index=index_name, body=document)
    

    
    
    
