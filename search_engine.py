# ==============================================================================
# search_engine.py
# This script orchestrates the movie search and ingestion pipelines.
# It uses a centralized SearchEngine class and handles parallel search queries.
# ==============================================================================
import sys
import threading
import concurrent.futures

# Assumed imports from other files in the project.
from event_bus import event_bus
from helpers.TMDB_helper import fetch_movies_from_tmdb, fetch_genres_from_tmdb, search_movies_from_tmdb
from helpers.weaviate_helper import save_embeddings_to_weaviate, search_weaviate_by_vector
# Import the Movie dataclass and helper functions from sqlite_helper.
from helpers.sqlite_helper import setup_sqlite_db, save_movies_to_sqlite, get_movies_by_ids_from_sqlite, get_movie_by_title_from_sqlite, get_existing_movie_ids, Movie
from helpers.model_loader import get_movie_titles_from_gemini

# ==============================================================================
# Centralized SearchEngine Class
# ==============================================================================
class SearchEngine:
    """
    Encapsulates all logic for the smart search and ingestion pipelines.
    This class handles everything from query refinement to data retrieval
    and result processing, and also manages its own event bus subscriptions.
    """
    def __init__(self, event_bus):
        """Initializes the engine with a reference to the event bus."""
        self.event_bus = event_bus
        # Subscribing to the event bus on initialization is a clean, centralized
        # way to ensure the engine is always ready to listen for events.
        self.subscribe_to_event_bus()

    def subscribe_to_event_bus(self):
        """Subscribes the engine's methods to all relevant events on the event bus."""
        # Subscribe to ingestion events
        self.event_bus.subscribe('start_ingestion', self.handle_start_ingestion_event)
        self.event_bus.subscribe('movies_fetched_for_ingestion', self.handle_movies_fetched_event)
        self.event_bus.subscribe('sqlite_data_saved', self.handle_sqlite_data_saved_event)
        self.event_bus.subscribe('ingestion_completed', self.handle_ingestion_completed_event)
        
        # Subscribe to search event
        self.event_bus.subscribe('start_search', self.run_search)

    # --- Handlers for Phase 1: Ingestion ---
    def handle_start_ingestion_event(self, delete_weaviate_collection: bool = False):
        """Starts the ingestion pipeline by fetching data from TMDB."""
        print("--- Phase 1: Starting Ingestion Pipeline ---")
        print("Fetching movies and genres from TMDB API...")
        # This returns a list of dictionaries from the TMDB API
        movies_data_dicts = fetch_movies_from_tmdb()
        genre_map = fetch_genres_from_tmdb()
        
        if not movies_data_dicts:
            print("No movies fetched from TMDB. Exiting ingestion.", file=sys.stderr)
            return

        self.event_bus.publish('movies_fetched_for_ingestion', movies_data_dicts=movies_data_dicts, genre_map=genre_map, delete_weaviate_collection=delete_weaviate_collection)

    def handle_movies_fetched_event(self, movies_data_dicts, genre_map, delete_weaviate_collection):
        """
        Converts movie dictionaries to Movie objects, saves them to SQLite,
        and triggers the next step.
        """
        print("Converting TMDB data to Movie objects...")
        # Convert each dictionary to a Movie dataclass object
        movies_data_objects = []
        for tmdb_dict in movies_data_dicts:
            try:
                movies_data_objects.append(Movie(
                    id=tmdb_dict['id'],
                    title=tmdb_dict['title'],
                    overview=tmdb_dict['overview'],
                    popularity=tmdb_dict['popularity'],
                    vote_average=tmdb_dict['vote_average'],
                    # FIX: Use .get() to prevent KeyError if vote_count is missing
                    vote_count=tmdb_dict.get('vote_count', 0),
                    release_date=tmdb_dict.get('release_date'),
                    poster_path=tmdb_dict.get('poster_path'),
                    genre_ids=tmdb_dict.get('genre_ids', []),
                    adult=tmdb_dict.get('adult', False),
                    backdrop_path=tmdb_dict.get('backdrop_path'),
                    original_language=tmdb_dict.get('original_language'),
                    original_title=tmdb_dict.get('original_title'),
                    video=tmdb_dict.get('video', False)
                ))
            except KeyError as e:
                print(f"Skipping movie due to missing key in TMDB response: {e}", file=sys.stderr)
                continue

        print("Saving movie metadata to SQLite...")
        setup_sqlite_db()
        # Now pass the list of Movie objects, not the dictionaries
        save_movies_to_sqlite(movies_data_objects, genre_map)

        self.event_bus.publish('sqlite_data_saved', movies_data=movies_data_objects, delete_weaviate_collection=delete_weaviate_collection)

    def handle_sqlite_data_saved_event(self, movies_data, delete_weaviate_collection):
        """Generates and saves embeddings to Weaviate, completing the ingestion phase."""
        print("Generating and saving movie embeddings to Weaviate...")
        save_embeddings_to_weaviate(movies_data, delete_collection=delete_weaviate_collection)

        self.event_bus.publish('ingestion_completed')

    def handle_ingestion_completed_event(self):
        """Prints a message when ingestion is completed."""
        print("\n--- Ingestion Pipeline Completed ---")
        
    # --- Helper Method for Phase 2: Smart Search ---
    def _check_and_fetch_movies(self, titles: list):
        """
        Checks for movie titles in SQLite. If not found, fetches them from TMDB
        and saves them to both SQLite and Weaviate in batches.
        
        Args:
            titles (list): A list of movie titles to search for.
            
        Returns:
            list: A list of all Movie objects found or fetched.
        """
        all_movies_data = []
        newly_fetched_movies = []
        
        for title in titles:
            # First, try to get the movie from SQLite by title.
            movie_from_sqlite = get_movie_by_title_from_sqlite(title)
            
            if movie_from_sqlite:
                print(f"Movie '{title}' found in SQLite cache. Using existing data.")
                all_movies_data.append(movie_from_sqlite)
            else:
                print(f"Movie '{title}' not found in SQLite. Fetching from TMDB.")
                # If not in SQLite, search on TMDB
                tmdb_results = search_movies_from_tmdb(title)
                
                if tmdb_results:
                    tmdb_dict = tmdb_results[0]
                    try:
                        new_movie_object = Movie(
                            id=tmdb_dict['id'],
                            title=tmdb_dict['title'],
                            overview=tmdb_dict['overview'],
                            popularity=tmdb_dict['popularity'],
                            vote_average=tmdb_dict['vote_average'],
                            vote_count=tmdb_dict.get('vote_count', 0),
                            release_date=tmdb_dict.get('release_date'),
                            poster_path=tmdb_dict.get('poster_path'),
                            genre_ids=tmdb_dict.get('genre_ids', []),
                            adult=tmdb_dict.get('adult', False),
                            backdrop_path=tmdb_dict.get('backdrop_path'),
                            original_language=tmdb_dict.get('original_language'),
                            original_title=tmdb_dict.get('original_title'),
                            video=tmdb_dict.get('video', False)
                        )
                        # Add the newly created movie object to a list
                        newly_fetched_movies.append(new_movie_object)
                        all_movies_data.append(new_movie_object)
                        
                    except KeyError as e:
                        print(f"Failed to create Movie object for '{title}' due to missing key: {e}", file=sys.stderr)
                        continue
                else:
                    print(f"No movies found on TMDB for title '{title}'.")
        
        # --- BATCH SAVES HERE, AFTER THE LOOP IS COMPLETE ---
        if newly_fetched_movies:
            print(f"\nSaving {len(newly_fetched_movies)} newly fetched movies to SQLite.")
            save_movies_to_sqlite(newly_fetched_movies, fetch_genres_from_tmdb())
            
            print(f"Saving {len(newly_fetched_movies)} newly fetched embeddings to Weaviate.")
            save_embeddings_to_weaviate(newly_fetched_movies, delete_collection=False)
        # ---------------------------------------------------
        
        return all_movies_data
        
    # --- Handlers for Phase 2: Smart Search ---
    def run_search(self, search_query: str, use_gemini: bool = True):
        """
        Executes a smart search from start to finish.
        
        Args:
            search_query (str): The initial user query string.
            use_gemini (bool): If True, uses Gemini to refine the query. 
                               If False, uses the original query for search.
        
        Returns:
            dict: A dictionary containing the TMDB and Weaviate search results.
        """
        thread_name = threading.current_thread().name
        print(f"\n[{thread_name}] --- Phase 2: Starting Smart Search Pipeline for '{search_query}' ---")
        
        if use_gemini:
            print(f"[{thread_name}] Generating movie titles using Gemini API...")
            try:
                generated_titles = get_movie_titles_from_gemini(search_query)
                if not generated_titles:
                    print(f"[{thread_name}] Gemini API did not return any titles. Falling back to original query.")
                    generated_titles = [search_query]
            except Exception as e:
                print(f"[{thread_name}] Error generating titles with Gemini: {e}. Falling back to original query.")
                generated_titles = [search_query]
        else:
            print(f"[{thread_name}] Skipping Gemini API. Using original query for search.")
            generated_titles = [search_query]
        
        print(f"[{thread_name}] Generated titles: {generated_titles}")

        print(f"[{thread_name}] Checking for titles in SQLite and fetching from TMDB if needed...")
        tmdb_results = self._check_and_fetch_movies(generated_titles)
        
        weaviate_results = search_weaviate_by_vector(search_query)
        
        # Now, process and return the results as a dictionary
        processed_weaviate_results = []
        if weaviate_results:
            found_movie_ids = [res['movie_id'] for res in weaviate_results]
            full_movies_data = get_movies_by_ids_from_sqlite(found_movie_ids)
            full_movies_dict = {movie.id: movie for movie in full_movies_data}
            
            for weaviate_res in weaviate_results:
                movie_id = weaviate_res['movie_id']
                if movie_id in full_movies_dict:
                    movie_data = full_movies_dict[movie_id]
                    processed_weaviate_results.append({
                        "title": movie_data.title,
                        "overview": movie_data.overview,
                        "distance": weaviate_res['metadata'].get('distance'),
                        "certainty": weaviate_res['metadata'].get('certainty'),
                        "poster_path": movie_data.poster_path,
                        "release_date": movie_data.release_date
                    })

        # Return a dictionary containing both sets of results
        return {
            "tmdb_results": [movie.to_dict() for movie in tmdb_results],
            "weaviate_results": processed_weaviate_results
        }