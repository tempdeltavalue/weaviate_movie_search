# helpers/weaviate_helper.py
import sys
import os
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Property, DataType
from weaviate.classes.query import MetadataQuery

from .model_loader import get_text_embedding, get_text_embeddings_batch
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

################### Weaviate Functions ###################
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

def connect_to_weaviate():
    """Connects to Weaviate and returns the client object."""
    if not all([WEAVIATE_URL, WEAVIATE_API_KEY]):
        print("Error: Weaviate environment variables are not set.", file=sys.stderr)
        return None
        
    try:
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=WEAVIATE_URL,
            auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
            skip_init_checks=True
        )
        return client
    except Exception as e:
        print(f"Failed to connect to Weaviate: {e}", file=sys.stderr)
        return None

def setup_weaviate_collection(client, delete_if_exists: bool = False):
    """
    Sets up the Weaviate collection for movie embeddings.
    If the collection exists, it is deleted only if `delete_if_exists` is True.
    Otherwise, the existing collection is returned.
    """
    collection_name = "Movie"
    
    # Check if the collection already exists
    if client.collections.exists(collection_name):
        if delete_if_exists:
            print(f"Collection '{collection_name}' exists. Deleting it...")
            client.collections.delete(collection_name)
        else:
            print(f"Collection '{collection_name}' already exists. Returning it.")
            return client.collections.get(collection_name)
    
    # If the collection does not exist (or was just deleted), create it
    print(f"Collection '{collection_name}' does not exist. Creating it now...")
    movies_collection = client.collections.create(
        name=collection_name,
        # We set vectorizer_config=None because we generate embeddings ourselves
        vectorizer_config=None,
        properties=[
            Property(name="movie_id", data_type=DataType.INT),
        ]
    )
    print("Weaviate collection created.")
    return movies_collection

def save_embeddings_to_weaviate(movies_data: list, delete_collection: bool = False):
    """
    Generates embeddings for movie overviews in a batch and saves them to Weaviate.
    """
    client = None
    try:
        client = connect_to_weaviate()
        if not client:
            return
        
        movies_collection = setup_weaviate_collection(client, delete_if_exists=delete_collection)
        
        # Filter movies with valid overviews
        valid_movies = [movie for movie in movies_data if movie.overview]
        overviews = [movie.overview for movie in valid_movies]
        movie_ids = [movie.id for movie in valid_movies]
        
        # Generate embeddings in a single batch call using the imported function
        print(f"\nGenerating {len(overviews)} embeddings in a batch...")
        embeddings_batch = get_text_embeddings_batch(overviews)
        
        # Prepare data for Weaviate batch import
        weaviate_batch_data = []
        for movie_id, vector in zip(movie_ids, embeddings_batch):
            weaviate_batch_data.append({
                "properties": {"movie_id": movie_id},
                "vector": vector
            })

        print(f"\nAdding {len(weaviate_batch_data)} embeddings to Weaviate...")
        with movies_collection.batch.dynamic() as batch:
            for item in weaviate_batch_data:
                batch.add_object(
                    properties=item["properties"],
                    vector=item["vector"]
                )
        print(f"Successfully saved {len(weaviate_batch_data)} movie embeddings to Weaviate.")
            
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
    finally:
        if client:
            client.close()
            print("Weaviate client connection closed.")

def search_weaviate_by_vector(query_text: str):
    """
    Performs a vector search on Weaviate based on a text query.
    Returns a list of matching movie IDs and their metadata.
    """
    client = None
    try:
        client = connect_to_weaviate()
        if not client:
            return []
            
        collection_name = "Movie"
        if not client.collections.exists(collection_name):
            print(f"Collection '{collection_name}' does not exist.", file=sys.stderr)
            return []

        movie_collection = client.collections.get(collection_name)
        
        # Generate the vector for the search query using the imported function
        query_vector = get_text_embedding(query_text)
        
        print(f"\nPerforming a vector search for: '{query_text}'...")
        
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
                "metadata": {
                    "distance": obj.metadata.distance,
                    "certainty": obj.metadata.certainty
                }
            })
        
        return found_results

    except Exception as e:
        print(f"An error occurred during Weaviate search: {e}", file=sys.stderr)
        return []
    finally:
        if client:
            client.close()
            print("Weaviate client connection closed.")

def main():
    """A test function to ensure the helper file is runnable."""
    print("This file contains helper functions for Weaviate.")
    print("Run `main.py` to see the full pipeline in action.")

if __name__ == "__main__":
    main()