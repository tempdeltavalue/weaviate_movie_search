# ==============================================================================
# app.py
# This script hosts the Flask API for the movie search engine.
# ==============================================================================
import os 
import traceback

from flask import Flask, request, jsonify, render_template
from search_engine import SearchEngine, SearchConfig
from dotenv import load_dotenv
from event_bus import event_bus

# Load environment variables from .env file
load_dotenv()

# ==============================================================================
# Flask Application
# ==============================================================================
app = Flask(__name__, template_folder='templates')

# Create a SearchEngine instance and subscribe it to the event bus
search_engine = SearchEngine(event_bus)


# ==============================================================================
# Flask Routes
# ==============================================================================
@app.route('/')
def index():
    """Renders the main search page from the templates folder."""
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    """Handles search requests and returns results as JSON."""
    try:
        data = request.get_json()
        query = data.get('query', '')
        
        # Get optional parameters from the request data
        start_year = data.get('start_year', None)
        end_year = data.get('end_year', None)
        enrich_from_tmdb = data.get('enrich_from_tmdb', False)
        use_gemini = data.get('use_gemini', True)

        if not query:
            return jsonify({"error": "No query provided"}), 400
        
        # Create a SearchConfig object from the provided parameters
        search_config = SearchConfig(
            start_year=start_year,
            end_year=end_year
        )
        
        print(f"Received search request for: '{query}' with config: {search_config} and enrich={enrich_from_tmdb}, use_gemini={use_gemini}")
        
        # Call the search engine's run_search method with all parameters
        results = search_engine.run_search(
            search_query=query, 
            search_config=search_config,
            enrich_from_tmdb=enrich_from_tmdb,
            use_gemini=use_gemini
        )
        
        return jsonify(results)
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": "An internal server error occurred"}), 500

# ==============================================================================
# Main Execution Block
# ==============================================================================
if __name__ == "__main__":
    # Initialize the SearchEngine and its event bus subscriptions
    # This is the only place it needs to be called
    search_engine.subscribe_to_event_bus()

    print("Initializing Flask app...")
    app.run(host='0.0.0.0', port=5000)