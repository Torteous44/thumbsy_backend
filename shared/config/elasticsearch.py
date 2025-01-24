# shared/config/elasticsearch.py

import os
from elasticsearch import Elasticsearch

# Environment variables, with defaults for local development
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_SCHEME = os.getenv("ES_SCHEME", "http")

# Initialize the Elasticsearch client
es_client = Elasticsearch(
    [
        {
            'host': ES_HOST,
            'port': int(ES_PORT),
            'scheme': ES_SCHEME
        }
    ]
)

def ping_elasticsearch() -> bool:
    """
    Return True if the ES client can ping the Elasticsearch cluster, else False.
    """
    try:
        return es_client.ping()
    except Exception:
        return False
