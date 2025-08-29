# ==============================================================================
# main.py
# Main entry point for the movie search application.
# ==============================================================================
import time
import sys
import os
import concurrent.futures
import logging
import re
from pprint import pprint

# --- CENTRALIZED LOGGING SETUP ---
# This block sets up the logging system, executed when the file is loaded.
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Clear existing handlers to prevent duplicates
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Set up a console handler for real-time output
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)
# --- END OF LOGGING SETUP BLOCK ---


from event_bus import EventBus
from helpers.postgres_helper import PostgresHelper, Movie
from helpers.weaviate_helper import WeaviateClient
from helpers.tmdb_client import TMDbClient
from search_engine import SearchEngine, SearchConfig


# Global variable to access the SearchEngine object from the event handler
search_engine: SearchEngine = None

# Helper function to create a safe filename from a query string
def _sanitize_filename(query: str) -> str:
    """Sanitizes a string to be a valid filename."""
    sanitized = re.sub(r'[^\w\s-]', '', query)
    sanitized = sanitized.replace(' ', '_')
    return sanitized.lower()[:50]  # Truncate to avoid overly long names

# --- Event Handlers ---
def handle_start_search_event(queries: list):
    """
    Event handler for starting concurrent search tasks.
    Creates a unique log file for each query.
    """
    logger = logging.getLogger(__name__)
    logger.info("\n--- Starting concurrent search tasks ---")
    
    start_time = time.time()
    all_results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_query = {
            executor.submit(
                # Pass a unique logger for each query
                search_engine.run_search, 
                query, 
                logger=logging.getLogger(f"query_logger_{_sanitize_filename(query)}"),
                enrich_from_tmdb=True
            ): query for query in queries
        }
        
        for future in concurrent.futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                data = future.result()
                all_results[query] = data
                logger.info(f"\n--- Search for '{query}' completed. Results collected. ---")
            except Exception as e:
                logger.error(f"Error for query '{query}': {e}", exc_info=True)
    
    end_time = time.time()
    logger.info(f"\n--- All search tasks have completed in {end_time - start_time:.2f} seconds. ---")
    
    logger.info("\n--- Final Collected Results ---")

    for query, results in all_results.items():
        logger.info(f"\n--- Results for query: '{query}' ---")
        
        if not results:
            logger.info("No results found.")
        else:
            tmdb_results = results.get('tmdb_results', [])
            weaviate_results = results.get('weaviate_results', [])

            if tmdb_results:
                logger.info("TMDb Results:")
                for movie in tmdb_results:
                    if isinstance(movie, dict) and 'title' in movie:
                        logger.info(f"- {movie['title']}")
                    else:
                        logger.info(f"- Unexpected data format: {movie}")

            if weaviate_results:
                logger.info("Weaviate Results:")
                for movie in weaviate_results:
                    if isinstance(movie, dict) and 'title' in movie:
                        distance = movie.get('distance', 'N/A')
                        certainty = movie.get('certainty', 'N/A')
                        logger.info(f"- {movie['title']} (Distance: {distance:.4f}, Certainty: {certainty:.4f})")
                    else:
                        logger.info(f"- Unexpected data format: {movie}")

def main():
    """Main function to run the application."""
    global search_engine
    
    logger = logging.getLogger(__name__)

    logger.info("--- Movie Search Application Starting ---")
    
    db_helper = PostgresHelper()
    db_helper.init_database()
    
    weaviate_helper = WeaviateClient()
    tmdb_client = TMDbClient()

    search_engine = SearchEngine(db=db_helper, weaviate_client=weaviate_helper, tmdb_client=tmdb_client)
    
    event_bus = EventBus()
    event_bus.subscribe("start_search", handle_start_search_event)
    
    search_queries = [
        'A horror movie with suspenseful elements',
        'I want to see movies by Gaspar Noe from the 2000s'
    ]
    
    logger.info("\n--- Running Search Tasks ---")
    event_bus.publish("start_search", queries=search_queries)
    
    logger.info("\n--- Application Finished ---")


if __name__ == "__main__":
    main()