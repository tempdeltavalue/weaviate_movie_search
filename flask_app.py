import os 
import traceback

from flask import Flask, request, jsonify, render_template
# Make sure to import the updated SearchEngine class
from search_engine import SearchEngine 
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

# --- NEW SERVER-SIDE FLAG TO CONTROL GEMINI USAGE ---
USE_GEMINI = True
# Set this to True to use Gemini for query refinement.
# Set this to False to skip Gemini and use the raw query.
# You must restart the server for this change to take effect.
# ----------------------------------------------------


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
        if not query:
            return jsonify({"error": "No query provided"}), 400
        
        # Call the search engine's run_search method and pass the server-side flag
        # The returned dictionary is correctly handled by jsonify.
        results = search_engine.run_search(query, use_gemini=USE_GEMINI)
        
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
    app.run(host='0.0.0.0', port=5001)