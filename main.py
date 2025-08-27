from search_engine import SearchEngine
import concurrent.futures

from dotenv import load_dotenv
from event_bus import event_bus

load_dotenv()

def main():
    """Sets up subscriptions and runs the pipelines."""
    
    # Flags to control script behavior
    RUN_INGESTION_PIPELINE = False
    DELETE_WEAVIATE_COLLECTION = False
    
    # Instantiate the SearchEngine class and subscribe all its methods to the event bus
    search_engine = SearchEngine(event_bus)
    search_engine.subscribe_to_event_bus()

    # The main thread can still publish ingestion events
    if RUN_INGESTION_PIPELINE:
        event_bus.publish('start_ingestion', delete_weaviate_collection=DELETE_WEAVIATE_COLLECTION)
    
    # --- Run multiple search queries in parallel ---
    search_queries = [
        "I want a movie about love",
        "I want a movie about a space mission that has a big secret",
        "A horror movie with suspenseful elements"
    ]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        print("\n--- Submitting search queries to thread pool... ---")
        # Submit the new class method to the executor
        futures = {executor.submit(search_engine.run_search, query): query for query in search_queries}
        
        # Wait for all tasks to finish.
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    main()
