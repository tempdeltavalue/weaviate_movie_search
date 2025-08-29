# ==============================================================================
# search_engine.py
# Implements the core search logic, combining vector search and API fallbacks.
# ==============================================================================
import threading
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import logging
import os

from helpers.weaviate_helper import WeaviateClient
from helpers.postgres_helper import PostgresHelper, Movie
from helpers.tmdb_client import TMDbClient
from helpers.model_loader import parse_user_query_with_gemini

import json
import re

@dataclass
class SearchConfig:
    pass

class SearchEngine:
    """Combines different search strategies to provide comprehensive results."""
    def __init__(self, db: PostgresHelper, weaviate_client: WeaviateClient, tmdb_client: TMDbClient):
        self.db = db
        self.weaviate_client = weaviate_client
        self.tmdb_client = tmdb_client

    def _filter_movie_data(self, movie_data: dict) -> dict:
        """
        Filters movie data from a source (like TMDb) to match the Movie model attributes.
        This prevents passing unexpected keyword arguments to the Movie constructor.
        """
        expected_attributes = [
            'id', 'title', 'release_date', 'overview', 'poster_path', 
            'vote_average', 'tmdb_id'
        ]
        
        filtered_data = {
            key: movie_data.get(key) for key in expected_attributes if movie_data.get(key) is not None
        }
        
        # Correction: check if release_date is not an empty string
        if 'release_date' in filtered_data and filtered_data['release_date'] == "":
            filtered_data['release_date'] = None

        # TMDb uses 'id', while we use 'tmdb_id'. We standardize it here.
        if 'id' in movie_data and 'tmdb_id' not in filtered_data:
            filtered_data['tmdb_id'] = movie_data['id']
            
        return filtered_data

    def run_search(self, search_query: str, logger: logging.Logger, search_config: SearchConfig = SearchConfig(), **kwargs) -> dict:
        """
        Executes a movie search pipeline based on the provided query and config.
        The logger argument is passed to enable per-query logging.
        """
        thread_name = threading.current_thread().name
        
        # Configure a unique file handler for this logger
        log_file_name = f"query_{logger.name.split('query_logger_')[-1]}.log"
        file_handler = logging.FileHandler(os.path.join('logs', log_file_name))
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        
        # Add the handler to the unique logger
        logger.addHandler(file_handler)
        logger.info(f"\n[{thread_name}] --- Phase 2: Starting Smart Search Pipeline for '{search_query}' ---")

        try:
            parsed_query = parse_user_query_with_gemini(search_query)
            logger.info(f"[{thread_name}] Query parsed by Gemini: {parsed_query}")

            final_results = {
                'weaviate_results': [],
                'tmdb_results': []
            }

            logger.info(f"\n \n parsed_query: {parsed_query}")
            
            # --- Optimized Logic: Check for specific data first. ---
            if parsed_query.get('director'):
                logger.info(f"[{thread_name}] Director '{parsed_query['director']}' found. Skipping Weaviate, going directly to TMDb.")
                
                tmdb_results = self.tmdb_client.get_director_movies_by_name(parsed_query['director'])
                
                newly_added_movies = []
                for movie_data in tmdb_results:
                    filtered_data = self._filter_movie_data(movie_data)
                    movie_id = filtered_data.get('tmdb_id')
                    
                    if movie_id and not self.db.movie_exists_in_db(movie_id):
                        logger.info(f"[{thread_name}] Movie with ID {movie_id} does not exist. Saving to DB...")
                        movie_obj = Movie(**filtered_data)
                        self.db.save_movies_to_db([movie_obj])
                        newly_added_movies.append(movie_obj)
                    
                if newly_added_movies:
                    logger.info(f"[{thread_name}] Ingesting {len(newly_added_movies)} new movie embeddings into Weaviate...")
                    self.weaviate_client.ingest_data(newly_added_movies, delete_weaviate_collection=False)

                final_results['tmdb_results'] = tmdb_results
                
            elif parsed_query.get('movie_titles'):
                logger.info(f"[{thread_name}] Gemini returned movie titles. Skipping Weaviate, going directly to TMDb.")

                tmdb_results = self.tmdb_client.search_multiple_titles(parsed_query['movie_titles'])

                newly_added_movies = []
                for movie_data in tmdb_results:
                    filtered_data = self._filter_movie_data(movie_data)
                    movie_id = filtered_data.get('tmdb_id')
                    
                    if movie_id and not self.db.movie_exists_in_db(movie_id):
                        logger.info(f"[{thread_name}] Movie with ID {movie_id} does not exist. Saving to DB...")
                        movie_obj = Movie(**filtered_data)
                        self.db.save_movies_to_db([movie_obj])
                        newly_added_movies.append(movie_obj)

                if newly_added_movies:
                    logger.info(f"[{thread_name}] Ingesting {len(newly_added_movies)} new movie embeddings into Weaviate...")
                    self.weaviate_client.ingest_data(newly_added_movies, delete_weaviate_collection=False)
                    
                final_results['tmdb_results'] = tmdb_results
            
            else:
                
                if kwargs.get('enrich_from_tmdb'):
                    logger.info(f"[{thread_name}] Weaviate results are not ideal. Falling back to TMDb API.")
                    
                    tmdb_results = self.tmdb_client.search_movies_from_tmdb(search_query)

                    newly_added_movies = []
                    for movie_data in tmdb_results:
                        filtered_data = self._filter_movie_data(movie_data)
                        movie_id = filtered_data.get('tmdb_id')
                        
                        if movie_id and not self.db.movie_exists_in_db(movie_id):
                            logger.info(f"[{thread_name}] Movie with ID {movie_id} does not exist. Saving to DB...")
                            movie_obj = Movie(**filtered_data)
                            self.db.save_movies_to_db([movie_obj])
                            newly_added_movies.append(movie_obj)

                    if newly_added_movies:
                        logger.info(f"[{thread_name}] Ingesting {len(newly_added_movies)} new movie embeddings into Weaviate...")
                        self.weaviate_client.ingest_data(newly_added_movies, delete_weaviate_collection=False)

                    final_results['tmdb_results'] = tmdb_results
                else:
                    logger.info(f"[{thread_name}] Weaviate results were good or TMDb enrichment is disabled. Skipping TMDb search.")

            logger.info(f"[{thread_name}] No specific director or movie titles found. Performing local semantic search...")
            
            weaviate_results_raw = self.weaviate_client.semantic_search(search_query)

            if weaviate_results_raw:
                full_movies_data = self.db.get_movies_by_ids_from_db([res['movie_id'] for res in weaviate_results_raw])

                semantic_results = []
                for raw_result in weaviate_results_raw:
                    movie_id = raw_result.get('movie_id')
                    full_movie = next((m for m in full_movies_data if m.id == movie_id), None)
                    if full_movie:
                        result_dict = full_movie.to_dict()
                        result_dict['distance'] = raw_result.get('distance')
                        result_dict['certainty'] = raw_result.get('certainty')
                        semantic_results.append(result_dict)
                final_results['weaviate_results'] = semantic_results
        
            logger.info(f"[{thread_name}] Search pipeline finished. Returning results.")
            return final_results
        finally:
            # IMPORTANT: Remove the handler after completion to prevent memory leaks
            logger.removeHandler(file_handler)