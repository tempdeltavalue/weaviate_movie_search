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