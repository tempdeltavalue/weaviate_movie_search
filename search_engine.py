# ==============================================================================
# search_engine.py
# This script orchestrates the movie search and ingestion pipelines.
# ==============================================================================
import sys
import threading
import concurrent.futures
from dataclasses import dataclass, field

from event_bus import event_bus
from helpers.TMDB_helper import fetch_movies_from_tmdb, fetch_genres_from_tmdb, search_movies_from_tmdb
from helpers.weaviate_helper import save_embeddings_to_weaviate, search_weaviate_by_vector
# --- UPDATED: Import the new GeminiPromptConfig class ---
from helpers.model_loader import get_movie_titles_from_gemini, GeminiPromptConfig

from helpers.postgres_helper import PostgresHelper, Movie

# ==============================================================================
# Search Configuration Class
# ==============================================================================
@dataclass
class SearchConfig:
    """A data class to hold all search-related parameters."""
    start_year: int = None
    end_year: int = None
    # Add other content-based filters here, like genre or rating

# ==============================================================================
# Centralized SearchEngine Class
# ==============================================================================
class SearchEngine:
    """
    Encapsulates all logic for the smart search and ingestion pipelines.
    """
    def __init__(self, event_bus):
        self.event_bus = event_bus
        self.db = PostgresHelper()
        self.db.init_database()
        self.subscribe_to_event_bus()

    def subscribe_to_event_bus(self):
        self.event_bus.subscribe('start_ingestion', self.handle_start_ingestion_event)
        self.event_bus.subscribe('movies_fetched_for_ingestion', self.handle_movies_fetched_event)
        self.event_bus.subscribe('sqlite_data_saved', self.handle_sqlite_data_saved_event)
        self.event_bus.subscribe('ingestion_completed', self.handle_ingestion_completed_event)
        self.event_bus.subscribe('start_search', self.run_search)

    def handle_start_ingestion_event(self, delete_weaviate_collection: bool = False):
        print("--- Phase 1: Starting Ingestion Pipeline ---")
        print("Fetching movies and genres from TMDB API...")
        movies_data_dicts = fetch_movies_from_tmdb()
        genre_map = fetch_genres_from_tmdb()
        
        if not movies_data_dicts:
            print("No movies fetched from TMDB. Exiting ingestion.", file=sys.stderr)
            return

        self.event_bus.publish('movies_fetched_for_ingestion', movies_data_dicts=movies_data_dicts, genre_map=genre_map, delete_weaviate_collection=delete_weaviate_collection)

    def handle_movies_fetched_event(self, movies_data_dicts, genre_map, delete_weaviate_collection):
        print("Converting TMDB data to Movie objects...")
        movies_data_objects = []
        for tmdb_dict in movies_data_dicts:
            try:
                movies_data_objects.append(Movie(
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
                ))
            except KeyError as e:
                print(f"Skipping movie due to missing key in TMDB response: {e}", file=sys.stderr)
                continue

        print("Saving movie metadata to PostgreSQL...")
        self.db.save_movies_to_db(movies_data_objects, genre_map)

        self.event_bus.publish('sqlite_data_saved', movies_data=movies_data_objects, delete_weaviate_collection=delete_weaviate_collection)

    def handle_sqlite_data_saved_event(self, movies_data, delete_weaviate_collection):
        print("Generating and saving movie embeddings to Weaviate...")
        save_embeddings_to_weaviate(movies_data, delete_collection=delete_weaviate_collection)

        self.event_bus.publish('ingestion_completed')

    def handle_ingestion_completed_event(self):
        print("\n--- Ingestion Pipeline Completed ---")
        
    def _run_tmdb_enrichment_search(self, search_query: str, use_gemini: bool = True, start_year: int = None, end_year: int = None):
        """Internal method to handle the TMDB/Gemini enrichment logic."""
        print(f"Using Gemini-powered enrichment search...")
        
        if use_gemini:
            print(f"Generating movie titles using Gemini API...")
            try:
                # --- UPDATED: Create a GeminiPromptConfig object and pass it ---
                prompt_config = GeminiPromptConfig(
                    query=search_query,
                    start_year=start_year,
                    end_year=end_year
                )
                generated_titles = get_movie_titles_from_gemini(prompt_config)
                if not generated_titles:
                    print(f"Gemini API did not return any titles. Falling back to original query.")
                    generated_titles = [search_query]
            except Exception as e:
                print(f"Error generating titles with Gemini: {e}. Falling back to original query.")
                generated_titles = [search_query]
        else:
            print(f"Skipping Gemini. Using original query for search.")
            generated_titles = [search_query]
        
        print(f"Generated titles for TMDB search: {generated_titles}")
        tmdb_results_objects = self._check_and_fetch_movies(generated_titles)
        return [movie.to_dict() for movie in tmdb_results_objects]

    def _check_and_fetch_movies(self, titles: list):
        """
        Checks for movie titles in the database. If not found, fetches them from TMDB
        and saves them.
        """
        all_movies_data = []
        newly_fetched_movies = []
        
        for title in titles:
            movie_from_db = self.db.get_movie_by_title_from_db(title)
            
            if movie_from_db:
                print(f"Movie '{title}' found in database cache. Using existing data.")
                all_movies_data.append(movie_from_db)
            else:
                print(f"Movie '{title}' not found in database. Fetching from TMDB.")
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
                        newly_fetched_movies.append(new_movie_object)
                        all_movies_data.append(new_movie_object)
                    except KeyError as e:
                        print(f"Failed to create Movie object for '{title}' due to missing key: {e}", file=sys.stderr)
                        continue
                else:
                    print(f"No movies found on TMDB for title '{title}'.")
        
        if newly_fetched_movies:
            print(f"\nSaving {len(newly_fetched_movies)} newly fetched movies to PostgreSQL.")
            self.db.save_movies_to_db(newly_fetched_movies, fetch_genres_from_tmdb())
            
            print(f"Saving {len(newly_fetched_movies)} newly fetched embeddings to Weaviate.")
            save_embeddings_to_weaviate(newly_fetched_movies, delete_collection=False)
        
        return all_movies_data

    def run_search(self, search_query: str, search_config: SearchConfig = SearchConfig(), enrich_from_tmdb: bool = False, use_gemini: bool = True):
        thread_name = threading.current_thread().name
        print(f"\n[{thread_name}] --- Phase 2: Starting Smart Search Pipeline for '{search_query}' ---")
        
        print(f"[{thread_name}] Performing a vector search on the local Weaviate database...")
        weaviate_results = search_weaviate_by_vector(search_query)

        processed_weaviate_results = []
        tmdb_results = []

        # Process Weaviate results first
        if weaviate_results:
            print(f"[{thread_name}] Found {len(weaviate_results)} matching movies in Weaviate. Fetching full data from PostgreSQL.")
            
            found_movie_ids = [res['movie_id'] for res in weaviate_results]
            full_movies_data = self.db.get_movies_by_ids_from_db(found_movie_ids)
            
            # --- Filter by year range if specified ---
            if search_config.start_year or search_config.end_year:
                filtered_movies = []
                for movie in full_movies_data:
                    if movie.release_date and len(movie.release_date) >= 4:
                        try:
                            release_year = int(movie.release_date[:4])
                            
                            year_matches = True
                            if search_config.start_year and release_year < search_config.start_year:
                                year_matches = False
                            if search_config.end_year and release_year > search_config.end_year:
                                year_matches = False

                            if year_matches:
                                filtered_movies.append(movie)
                        except (ValueError, TypeError):
                            continue
                
                full_movies_data = filtered_movies
                print(f"[{thread_name}] Filtered results by year range from {search_config.start_year or 'any'} to {search_config.end_year or 'any'}.")

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

        # This part runs the TMDB enrichment process independently
        if enrich_from_tmdb:
            print(f"[{thread_name}] 'enrich_from_tmdb' is True. Running TMDB enrichment process...")
            tmdb_results = self._run_tmdb_enrichment_search(search_query, use_gemini=use_gemini, start_year=search_config.start_year, end_year=search_config.end_year)

        return {
            "tmdb_results": tmdb_results,
            "weaviate_results": processed_weaviate_results
        }