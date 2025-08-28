# ==============================================================================
# main.py
# This script orchestrates the movie search and ingestion pipelines.
# It uses a centralized SearchEngine class and handles parallel search queries.
# ==============================================================================
import sys
import concurrent.futures
import time

# Import the updated SearchEngine class and the new SearchConfig class
from search_engine import SearchEngine, SearchConfig
from dotenv import load_dotenv
from event_bus import event_bus

load_dotenv()

# ==============================================================================
# Main Execution Block
# ==============================================================================

def main():
    """Sets up subscriptions and runs the pipelines."""
    
    # Flags to control script behavior
    # Set this to True to run the ingestion pipeline first.
    RUN_INGESTION_PIPELINE = False
    DELETE_WEAVIATE_COLLECTION = False

    # Instantiate the SearchEngine class and subscribe all its methods to the event bus
    search_engine = SearchEngine(event_bus)

    # The main thread can still publish ingestion events
    if RUN_INGESTION_PIPELINE:
        print("--- Running Ingestion Pipeline ---")
        event_bus.publish('start_ingestion', delete_weaviate_collection=DELETE_WEAVIATE_COLLECTION)
        time.sleep(5)
    
    # --- Run multiple search queries in parallel ---
    # Each item is a dictionary with all task parameters
    search_tasks = [
        {
            "query": "I want a movie about love",
            "search_config": SearchConfig(),
            "enrich_from_tmdb": False,
            "use_gemini": True
        },
        {
            "query": "I want a movie about a space mission that has a big secret",
            "search_config": SearchConfig(),
            "enrich_from_tmdb": True,
            "use_gemini": True
        },
        {
            "query": "A horror movie with suspenseful elements",
            "search_config": SearchConfig(start_year=2015, end_year=2024),
            "enrich_from_tmdb": False,
            "use_gemini": True
        }
    ]
    
    # We will store the results here
    all_search_results = {}
    
    print("\n--- Starting concurrent search tasks ---")
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit the run_search method for each task defined in the dictionary
        futures = {
            executor.submit(
                search_engine.run_search, 
                task["query"], 
                task["search_config"],
                enrich_from_tmdb=task["enrich_from_tmdb"],
                use_gemini=task["use_gemini"]
            ): task["query"] for task in search_tasks
        }
        
        # Iterate over the completed futures as they finish.
        for future in concurrent.futures.as_completed(futures):
            original_query = futures[future]
            try:
                result = future.result() 
                all_search_results[original_query] = result
                print(f"\n--- Search for '{original_query}' completed. Results collected. ---")
                
            except Exception as e:
                print(f"Error for query '{original_query}': {e}")
                
    end_time = time.time()
    print(f"\n--- All search tasks have completed in {end_time - start_time:.2f} seconds. ---")
    
    # --- UPDATED: Process and display all collected results ---
    print("\n--- Final Collected Results ---")
    for query, results in all_search_results.items():
        print(f"\nQuery: '{query}'")
        
        # Access the TMDB results
        tmdb_results = results.get('tmdb_results', [])
        print("\n--- TMDB Search Results: ---")
        if not tmdb_results:
            print("No movies found from the Gemini-generated titles.")
        else:
            for i, movie in enumerate(tmdb_results[:5]):
                print(f"{i+1}. {movie['title']}")
        print("-" * 20)

        # Access the Weaviate results
        weaviate_results = results.get('weaviate_results', [])
        print("\n--- Semantic Search Results: ---")
        if not weaviate_results:
            print("No matching movies found in Weaviate.")
        else:
            for i, movie_result in enumerate(weaviate_results):
                print(f"{i+1}. Title: {movie_result.get('title')}")
                print(f"   Overview: {movie_result.get('overview')}")
                distance = movie_result.get('distance')
                certainty = movie_result.get('certainty')
                if distance is not None:
                    print(f"   Weaviate Distance: {distance:.4f}")
                if certainty is not None:
                    print(f"   Weaviate Certainty: {certainty:.4f}")
                print("-" * 20)
                
if __name__ == "__main__":
    main()