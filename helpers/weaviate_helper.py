# ==============================================================================
# helpers/weaviate_helper.py
# This script manages the Weaviate database client and operations.
# ==============================================================================
import sys
import os
import weaviate
from typing import List, Dict, Any
from dataclasses import dataclass, field
from dotenv import load_dotenv

from weaviate.classes.init import Auth
from weaviate.classes.config import Property, DataType, Configure
from weaviate.classes.query import MetadataQuery

from .model_loader import get_text_embedding, get_text_embeddings_batch
from .postgres_helper import Movie

load_dotenv()

################### Weaviate Helper Functions ###################
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

def _connect_to_weaviate():
    """Connects to Weaviate and returns the client object."""
    if not all([WEAVIATE_URL, WEAVIATE_API_KEY]):
        print("Error: Weaviate environment variables are not set.", file=sys.stderr)
        return None
        
    try:
        print(f"Attempting to connect to Weaviate Cloud at {WEAVIATE_URL}...")
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=WEAVIATE_URL,
            auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
            skip_init_checks=True
        )

        if client.is_ready():
            print("Successfully connected to Weaviate and it is ready.")
            return client
        else:
            print("Weaviate client is not ready.", file=sys.stderr)
            client.close()
            return None
    except Exception as e:
        print(f"Failed to connect to Weaviate: {e}", file=sys.stderr)
        return None

def _setup_weaviate_collection(client, delete_if_exists: bool = False):
    """
    Sets up the Weaviate collection for movie embeddings.
    """
    collection_name = "Movie"
    
    if client.collections.exists(collection_name):
        if delete_if_exists:
            print(f"Collection '{collection_name}' exists. Deleting it...")
            client.collections.delete(collection_name)
        else:
            print(f"Collection '{collection_name}' already exists. Returning it.")
            return client.collections.get(collection_name)
    
    print(f"Collection '{collection_name}' does not exist. Creating it now...")
    movies_collection = client.collections.create(
        name=collection_name,
        vectorizer_config=Configure.Vectorizer.none(),
        properties=[
            Property(name="movie_id", data_type=DataType.INT),
        ],
        inverted_index_config=Configure.inverted_index(
            index_null_state=True
        ),
    )
    print("Weaviate collection created.")
    return movies_collection

def _save_embeddings_to_weaviate(client, movies_data: list, delete_collection: bool = False):
    """
    Generates embeddings for movie overviews in a batch and saves them to Weaviate.
    """
    try:
        movies_collection = _setup_weaviate_collection(client, delete_if_exists=delete_collection)
        
        valid_movies = [movie for movie in movies_data if movie.overview]
        overviews = [movie.overview for movie in valid_movies]
        
        print(f"Generating {len(overviews)} embeddings in a batch...")
        embeddings_batch = get_text_embeddings_batch(overviews)
        
        print(f"Starting batch ingestion into '{movies_collection.name}'...")
        with movies_collection.batch.dynamic() as batch:
            for movie, vector in zip(valid_movies, embeddings_batch):
                batch.add_object(
                    properties={
                        "movie_id": movie.id,
                    },
                    vector=vector
                )
        print("Ingestion completed.")
            
    except Exception as e:
        print(f"An error occurred during ingestion: {e}", file=sys.stderr)
        sys.exit(1)
        
def _search_weaviate_by_vector(client, query_text: str):
    """
    Performs a vector search on Weaviate based on a text query.
    """
    try:
        collection_name = "Movie"
        if not client.collections.exists(collection_name):
            print(f"Collection '{collection_name}' does not exist.", file=sys.stderr)
            return []

        movie_collection = client.collections.get(collection_name)
        
        query_vector = get_text_embedding(query_text)
        if not query_vector:
            return []

        print(f"Performing a vector search for: '{query_text}'...")
        
        results = movie_collection.query.near_vector(
            near_vector=query_vector,
            limit=10,
            return_properties=["movie_id"],
            return_metadata=MetadataQuery(distance=True, certainty=True)
        )
        
        found_results = []
        for obj in results.objects:
            found_results.append({
                "movie_id": obj.properties['movie_id'],
                "distance": obj.metadata.distance,
                "certainty": obj.metadata.certainty
            })
        
        return found_results

    except Exception as e:
        print(f"An error occurred during Weaviate search: {e}", file=sys.stderr)
        return []

################### Weaviate Client Class ###################
class WeaviateClient:
    def __init__(self):
        self.client = _connect_to_weaviate()

    def ingest_data(self, movies: List[Movie], delete_weaviate_collection: bool = False):
        if not self.client:
            print("Weaviate client not connected. Skipping ingestion.")
            return

        _save_embeddings_to_weaviate(self.client, movies, delete_weaviate_collection)

    def semantic_search(self, query: str) -> List[dict]:
        if not self.client:
            print("Weaviate client not connected. Skipping search.")
            return []
        
        return _search_weaviate_by_vector(self.client, query)

    def close(self):
        if self.client:
            try:
                self.client.close()
                print("Weaviate client closed.")
            except Exception as e:
                print(f"Error closing Weaviate client: {e}", file=sys.stderr)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
