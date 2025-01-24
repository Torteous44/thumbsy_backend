import os
from elasticsearch import Elasticsearch

# Retrieve host and port from environment variables; fallback to localhost:9200 if not set
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", "9200"))

# Initialize the Elasticsearch client
import os
from elasticsearch import Elasticsearch

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", "9200"))

# Note the "scheme" key explicitly set to "http"
es_client = Elasticsearch(
    [
        {
            "host": ES_HOST,
            "port": ES_PORT,
            "scheme": "http"
        }
    ]
)


def ping_elasticsearch() -> bool:
    """
    Return True if the ES client can ping the Elasticsearch cluster, else False.
    """
    return es_client.ping()

def create_index(index_name: str):
    """
    Creates an index in Elasticsearch if it doesn't already exist.
    """
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name)
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")

def index_document(index_name: str, doc_id: str, body: dict):
    """
    Index (insert or update) a single document in Elasticsearch.
    """
    response = es_client.index(index=index_name, id=doc_id, body=body)
    return response

def search_documents(index_name: str, query: dict) -> dict:
    """
    Searches documents in the given index using a specified query.
    """
    response = es_client.search(index=index_name, body=query)
    return response
