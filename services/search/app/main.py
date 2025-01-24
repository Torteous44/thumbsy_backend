# services/search/app/main.py (for example)
from fastapi import FastAPI
from .utils.elasticsearch import (
    ping_elasticsearch,
    create_index,
    index_document,
    search_documents
)

app = FastAPI()

@app.on_event("startup")
def startup_event():
    if ping_elasticsearch():
        print("Successfully connected to Elasticsearch!")
    else:
        raise Exception("Failed to connect to Elasticsearch.")

    # Create or verify the index
    create_index(index_name="products")

@app.post("/index-sample")
def index_sample_document():
    sample_data = {
        "title": "Awesome Headphones",
        "description": "Noise-cancelling wireless headphones with rich bass.",
        "price": 199.99,
        "tags": ["electronics", "audio", "headphones"]
    }
    response = index_document("products", doc_id="1", body=sample_data)
    return {"result": response["result"], "id": response["_id"]}

@app.get("/search")
def search_headphones():
    query = {
        "query": {
            "match": {
                "description": "headphones"
            }
        }
    }
    results = search_documents("products", query)
    return results
