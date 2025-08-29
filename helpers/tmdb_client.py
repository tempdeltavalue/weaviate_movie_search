# ==============================================================================
# helpers/tmdb_client.py
# Helper class to interact with The Movie Database (TMDb) API.
# ==============================================================================
import os
import requests
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class TMDbClient:
    def __init__(self):
        self.api_key = os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB_API_KEY environment variable not set.")
        self.base_url = "https://api.themoviedb.org/3"

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Helper to make a GET request to the TMDb API."""
        url = f"{self.base_url}/{endpoint}"
        params.update({"api_key": self.api_key})
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def search_movies_from_tmdb(self, query: str) -> List[Dict[str, Any]]:
        """Searches for movies on TMDb by a given query."""
        endpoint = "search/movie"
        params = {"query": query}
        data = self._make_request(endpoint, params)
        return data.get("results", [])

    def search_person(self, query: str) -> List[Dict[str, Any]]:
        """Searches for a person (e.g., director) on TMDb."""
        endpoint = "search/person"
        params = {"query": query}
        data = self._make_request(endpoint, params)
        return data.get("results", [])

    def get_person_movie_credits(self, person_id: int) -> List[Dict[str, Any]]:
        """Gets the movie credits for a given person."""
        endpoint = f"person/{person_id}/movie_credits"
        data = self._make_request(endpoint, params={})
        # Filter for director credits
        director_movies = [
            cast_member for cast_member in data.get("crew", [])
            if cast_member.get("job") == "Director"
        ]
        return director_movies
        
    def get_movie_details(self, movie_id: int) -> Dict[str, Any]:
        """Gets detailed information for a specific movie."""
        endpoint = f"movie/{movie_id}"
        return self._make_request(endpoint, params={})
        
    def get_director_movies_by_name(self, director_name: str) -> List[Dict[str, Any]]:
        """
        Finds a director by name and returns their top movies.
        """
        print(f"Searching for director '{director_name}' on TMDb...")
        person_results = self.search_person(director_name)

        if not person_results:
            print(f"No person found for name '{director_name}'.")
            return []

        director_id = person_results[0].get('id')
        print(f"Found person '{person_results[0].get('name')}' with ID {director_id}.")
        
        movies = self.get_person_movie_credits(director_id)
        
        # Sort by popularity to get the most relevant movies
        movies.sort(key=lambda x: x.get('popularity', 0), reverse=True)
        
        print(f"Found {len(movies)} movies directed by {director_name}.")
        
        return movies
    
    def search_multiple_titles(self, titles: List[str]) -> List[Dict[str, Any]]:
        """Searches for multiple movie titles and returns a combined list of results."""
        all_results = []
        for title in titles:
            results = self.search_movies_from_tmdb(title)
            if results:
                # We only need the first result for a direct title search
                movie_data = results[0]
                all_results.append(movie_data)
        return all_results