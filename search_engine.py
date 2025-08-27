# ==============================================================================
# Centralized SearchEngine Class
# ==============================================================================
from helpers.TMDB_helper import fetch_movies_from_tmdb, fetch_genres_from_tmdb, search_movies_from_tmdb
from helpers.weaviate_helper import save_embeddings_to_weaviate, search_weaviate_by_vector
from helpers.sqlite_helper import setup_sqlite_db, save_movies_to_sqlite, get_movies_by_ids_from_sqlite, get_existing_movie_ids
from helpers.model_loader import generate_search_query

import threading

class SearchEngine:
    """
    Encapsulates all logic for the smart search and ingestion pipelines.
    This class now handles everything from query refinement to data retrieval
    and result processing, and also manages its own event bus subscriptions.
    """
    def __init__(self, event_bus):
        """Initializes the engine with a reference to the event bus."""
        self.event_bus = event_bus

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
        movies_data = fetch_movies_from_tmdb()
        genre_map = fetch_genres_from_tmdb()
        
        if not movies_data:
            print("No movies fetched from TMDB. Exiting ingestion.", file=sys.stderr)
            return

        self.event_bus.publish('movies_fetched_for_ingestion', movies_data=movies_data, genre_map=genre_map, delete_weaviate_collection=delete_weaviate_collection)

    def handle_movies_fetched_event(self, movies_data, genre_map, delete_weaviate_collection):
        """Saves movie metadata to SQLite and triggers the next step."""
        print("Saving movie metadata to SQLite...")
        setup_sqlite_db()
        save_movies_to_sqlite(movies_data, genre_map)

        self.event_bus.publish('sqlite_data_saved', movies_data=movies_data, delete_weaviate_collection=delete_weaviate_collection)

    def handle_sqlite_data_saved_event(self, movies_data, delete_weaviate_collection):
        """Generates and saves embeddings to Weaviate, completing the ingestion phase."""
        print("Generating and saving movie embeddings to Weaviate...")
        save_embeddings_to_weaviate(movies_data, delete_collection=delete_weaviate_collection)

        self.event_bus.publish('ingestion_completed')

    def handle_ingestion_completed_event(self):
        """Prints a message when ingestion is completed."""
        print("\n--- Ingestion Pipeline Completed ---")
        
    # --- Handlers for Phase 2: Smart Search ---
    def run_search(self, search_query: str):
        """
        Executes a smart search from start to finish, including query refinement,
        fetching results from TMDB and Weaviate, and printing the output.
        
        Args:
            search_query (str): The initial user query string.
        """
        thread_name = threading.current_thread().name
        print(f"\n[{thread_name}] --- Phase 2: Starting Smart Search Pipeline for '{search_query}' ---")
        
        generated_query = generate_search_query(search_query)
        print(f"[{thread_name}] Original query: '{search_query}'")
        print(f"[{thread_name}] Generated search query: '{generated_query}'")

        tmdb_results = search_movies_from_tmdb(generated_query)
        weaviate_results = search_weaviate_by_vector(generated_query)
        
        # Now, process and print the results directly within this method
        self._process_and_print_results(generated_query, tmdb_results, weaviate_results)

    def _process_and_print_results(self, search_query, tmdb_results, weaviate_results):
        """Processes and prints the search results."""
        print(f"\n--- Results for Query: '{search_query}' ---")
        print("\n--- TMDB Search Results: ---")
        if not tmdb_results:
            print("No movies found on TMDB for the given query.")
        else:
            for i, movie in enumerate(tmdb_results[:5]):
                print(f"{i+1}. {movie.get('title', 'N/A')}")
        print("----------------------------\n")
        
        if not weaviate_results:
            print("No matching movies found in Weaviate.")
            return

        found_movie_ids = [res['movie_id'] for res in weaviate_results]
        full_movies_data = get_movies_by_ids_from_sqlite(found_movie_ids)

        final_results = []
        for weaviate_res in weaviate_results:
            movie_id = weaviate_res['movie_id']
            for movie_data in full_movies_data:
                if movie_data and movie_data.id == movie_id:
                    final_results.append({
                        "movie": movie_data,
                        "metadata": weaviate_res['metadata']
                    })
                    break
        
        print("\nFinal Semantic Search Results:")
        for i, result in enumerate(final_results):
            movie = result['movie']
            metadata = result['metadata']
            print(f"{i+1}. Title: {movie.title}")
            print(f"   Overview: {movie.overview}")
            print(f"   Weaviate Distance: {metadata['distance']:.4f}")
            print(f"   Weaviate Certainty: {metadata['certainty']:.4f}")
            print("-" * 20)