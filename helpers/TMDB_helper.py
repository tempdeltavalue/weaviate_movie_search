import requests
import os 
import sys  
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# ==================== ENVIRONMENT VARIABLES & CONFIG ====================
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_DISCOVER_URL = os.getenv("TMDB_DISCOVER_URL")
TMDB_GENRE_URL = os.getenv("TMDB_GENRE_URL")
TMDB_SEARCH_URL = os.getenv("TMDB_SEARCH_URL")

# ==================== TMDB HELPER FUNCTIONS ====================
def fetch_movies_from_tmdb(page: int = 1) -> list:
    """Fetches a list of popular movies from the TMDB discover endpoint."""
    params = {
        'api_key': TMDB_API_KEY,
        'sort_by': 'popularity.desc',
        'include_adult': 'false',
        'page': page,
    }
    try:
        response = requests.get(TMDB_DISCOVER_URL, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching movies from TMDB: {e}", file=sys.stderr)
        return []

def fetch_genres_from_tmdb() -> dict:
    """Fetches a mapping of genre IDs to genre names from TMDB."""
    params = {
        'api_key': TMDB_API_KEY,
    }
    try:
        response = requests.get(TMDB_GENRE_URL, params=params)
        response.raise_for_status()
        genres = response.json().get("genres", [])
        return {genre["id"]: genre["name"] for genre in genres}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching genres from TMDB: {e}", file=sys.stderr)
        return {}
        
def search_movies_from_tmdb(query: str) -> list:
    """Performs a text-based search for movies on TMDB."""
    if not query:
        print("Error: Search query is empty.", file=sys.stderr)
        return []
        
    params = {
        'api_key': TMDB_API_KEY,
        'query': query,
        'include_adult': 'false',
    }
    try:
        response = requests.get(TMDB_SEARCH_URL, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from TMDB search: {e}", file=sys.stderr)
        return []

# ==================== MAIN TEST BLOCK ====================
def main():
    """
    Demonstrates the full pipeline:
    1. Generates a concise query from a long user query using Hugging Face.
    2. Uses the generated query to search for movies on TMDB.
    """
    print("Starting a test of the smart search pipeline...")

    # A long, descriptive user query
    user_query = "I want a movie about a space mission that has a big secret"

    # Step 1: Generate a concise query using the NLP model
    generated_query = generate_search_query(user_query)
    print(f"\nOriginal query: '{user_query}'")
    print(f"Generated search query: '{generated_query}'")
    
    # Step 2: Use the generated query to search on TMDB
    print("\nSearching TMDB with the generated query...")
    tmdb_results = search_movies_from_tmdb(generated_query)

    if tmdb_results:
        print(f"\nFound {len(tmdb_results)} movies on TMDB for the query: '{generated_query}'")
        for i, movie in enumerate(tmdb_results[:10]):
            print(f"{i+1}. {movie.get('title', 'N/A')}")
    else:
        print(f"\nNo movies found on TMDB for the query: '{generated_query}'.")


if __name__ == "__main__":
    main()