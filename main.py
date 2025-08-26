# -*- coding: utf-8 -*-
import requests
import sys
import json
import sqlite3
import torch
import torch.nn.functional as F

# Import database functions and MovieModel from the separate database.py file
from database import save_movies_to_db, get_movies_from_db, MovieModel
# Import text processing functions from the new text_processor.py file
from text_processor import create_model_and_tokenizer, get_text_embedding

# --- TMDB API Configuration ---
# IMPORTANT: You must replace this with your personal TMDB API key.
# A Google API key will NOT work here.
API_KEY = "123123"
TMDB_GENRE_URL = "https://api.themoviedb.org/3/genre/movie/list"
TMDB_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"

# --- Faceted Search Query ---
# Enter your search query here. This could be a genre name.
SEARCH_GENRE = "Action"
SEARCH_QUERY_TEXT = "A spy movie with a lot of explosions"

def get_genre_mapping(api_key: str) -> dict:
    """
    Fetches the mapping of genre IDs to genre names from the TMDB API.
    Returns a dictionary like {28: 'Action', 12: 'Adventure', ...}.
    """
    params = {
        "api_key": api_key,
        "language": "en-US"
    }
    try:
        response = requests.get(TMDB_GENRE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return {genre['id']: genre['name'] for genre in data.get('genres', [])}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching genre list: {e}", file=sys.stderr)
        return {}

def calculate_cosine_similarity(vec1: torch.Tensor, vec2: torch.Tensor) -> float:
    """
    Calculates the cosine similarity between two PyTorch tensors.
    """
    # Reshape to 1D tensors if they are 2D (batch size 1)
    vec1 = vec1.squeeze(0)
    vec2 = vec2.squeeze(0)
    
    # Calculate cosine similarity
    return F.cosine_similarity(vec1, vec2, dim=0).item()

def search_and_display_movies(api_key: str, genre_name: str) -> list[MovieModel]:
    """
    Performs a movie search by genre, saves them to the DB, then retrieves
    them from the DB and returns a list of MovieModel objects.
    """
    # Check if the API key is the default placeholder or looks like a Google API key
    if not api_key or api_key == "YOUR_TMDB_API_KEY":
        print("Error: Please replace 'YOUR_TMDB_API_KEY' with your actual TMDB API key.")
        return []
    if api_key.startswith("AIzaSy"):
        print("Error: The provided API key appears to be a Google API key. Please use a valid TMDB API key.")
        return []
    
    # 1. Get genre mapping to find the ID for the given genre name
    genre_map = get_genre_mapping(api_key)
    genre_id = None
    for gid, name in genre_map.items():
        if name.lower() == genre_name.lower():
            genre_id = gid
            break
    
    if not genre_id:
        print(f"Genre '{genre_name}' not found. Please choose from available genres.")
        return []

    params = {
        "api_key": api_key,
        "language": "en-US",
        "with_genres": str(genre_id)
    }

    try:
        # 2. Send an HTTP GET request to the TMDB Discover API
        print(f"Searching for movies in genre '{genre_name}'...")
        response = requests.get(TMDB_DISCOVER_URL, params=params)
        response.raise_for_status()

        data = response.json()
        movies = data.get("results", [])

        if not movies:
            print(f"No movies found for the genre '{genre_name}'.")
            return []
        
        # 3. Save the results to the database
        save_movies_to_db(movies, genre_map)

        # 4. Retrieve the movies from the database as a list of MovieModel objects
        movies_from_db = get_movies_from_db()

        return movies_from_db

    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

# --- Execution ---
if __name__ == "__main__":
    results = search_and_display_movies(API_KEY, SEARCH_GENRE)
    if results:
        # Get the tokenizer and model from the text_processor module
        tokenizer, model = create_model_and_tokenizer()
        
        # Get embedding for the search query
        query_embedding = get_text_embedding(SEARCH_QUERY_TEXT, tokenizer, model)
        print(f"Search Query: '{SEARCH_QUERY_TEXT}'")
        print(f"Query embedding shape: {query_embedding.shape}\n")
        
        print(f"Retrieved {len(results)} movies from the database for '{SEARCH_GENRE}':\n")
        
        movies_with_similarity = []
        for movie in results:
            if movie.overview and len(movie.overview) > 10:
                movie_embedding = get_text_embedding(movie.overview, tokenizer, model)
                similarity = calculate_cosine_similarity(query_embedding, movie_embedding)
                movies_with_similarity.append({'movie': movie, 'similarity': similarity})
            else:
                movies_with_similarity.append({'movie': movie, 'similarity': -1.0}) # Use -1.0 for movies without an overview
        
        # Sort movies by similarity in descending order
        movies_with_similarity.sort(key=lambda x: x['similarity'], reverse=True)

        for i, item in enumerate(movies_with_similarity):
            movie = item['movie']
            similarity = item['similarity']
            
            print(f"{i+1}. {movie.title}")
            print(f"   Release Date: {movie.release_date}")
            print(f"   Vote Average: {movie.vote_average} / 10")
            print(f"   Popularity: {movie.popularity}")
            print(f"   Original Language: {movie.original_language.upper()}")
            print(f"   Genres: {movie.genres}")
            print(f"   Overview: {movie.overview}")
            if similarity != -1.0:
                print(f"   Cosine Similarity to query: {similarity:.4f}")
            else:
                print("   Cosine Similarity: N/A (Overview not available)")
            print("-" * 20)
    else:
        print("No results to display.")
