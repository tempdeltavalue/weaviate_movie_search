# ==============================================================================
# helpers/model_loader.py
# ==============================================================================
import sys
import os 
import json
import torch
import requests
import traceback
import re
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# Global Model and API Configuration
# ==============================================================================
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
_sentence_model_instance = None
API_KEY = os.getenv("API_KEY_GEMINI")
if not API_KEY:
    print("Warning: API_KEY_GEMINI environment variable is not set.", file=sys.stderr)
API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key=" + str(API_KEY)

# A list of common words to ignore in keyword extraction
STOP_WORDS = {
    'i', 'want', 'to', 'see', 'a', 'an', 'the', 'by', 'from', 'in', 'and', 'with', 'about', 'movie', 'movies', 'film', 'films', 'director', 'director', 'starring'
}

# ==============================================================================
# Functions for Text Embeddings (for Semantic Search)
# ==============================================================================
def get_text_embedding(text: str) -> List[float]:
    """Generates an embedding for a single text using the loaded model."""
    global _sentence_model_instance
    if _sentence_model_instance is None:
        try:
            device = "cpu"
            _sentence_model_instance = SentenceTransformer(MODEL_NAME, device=device)
            print("Embedding model loaded successfully.")
            print(f"Model is running on device: {device}")
        except Exception as e:
            print(f"Failed to load embedding model: {e}", file=sys.stderr)
            _sentence_model_instance = None

    if _sentence_model_instance:
        try:
            embedding = _sentence_model_instance.encode(text, convert_to_tensor=True)
            return embedding.tolist()
        except Exception as e:
            print(f"Error encoding text: {e}", file=sys.stderr)
            return None
    print("Could not generate embedding due to missing model.", file=sys.stderr)
    return None

def get_text_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Generates a batch of embeddings for a list of strings."""
    global _sentence_model_instance
    if _sentence_model_instance is None:
        try:
            device = "cpu"
            _sentence_model_instance = SentenceTransformer(MODEL_NAME, device=device)
            print("Embedding model loaded successfully.")
            print(f"Model is running on device: {device}")
        except Exception as e:
            print(f"Failed to load embedding model: {e}", file=sys.stderr)
            _sentence_model_instance = None
            
    if _sentence_model_instance:
        try:
            embeddings = _sentence_model_instance.encode(texts, convert_to_tensor=True)
            return embeddings.tolist()
        except Exception as e:
            print(f"Error encoding batch texts: {e}", file=sys.stderr)
            return []
    print("Could not generate embeddings due to missing model.", file=sys.stderr)
    return []

# ==============================================================================
# Functions for Query Parsing using Gemini API
# ==============================================================================
def parse_user_query_with_gemini(query: str) -> Dict[str, Any]:
    """
    Uses Gemini to parse a natural language query into a structured JSON object.
    It first tries to find a director and years. If no specific information is found,
    it falls back to generating a list of movie titles.
    """
    
    # Prompt for parsing the query for specific details
    parsing_prompt = (
        f"You are a movie search engine. Your task is to extract relevant information "
        f"from the user's query and format it into a JSON object. "
        f"The JSON should contain the following keys: 'director' (string, if a director is mentioned), "
        f"'start_year' (integer), 'end_year' (integer). If a key is not found, its value should be null. "
        f"Example for 'I want all movies by Christopher Nolan from 2000 to 2010': "
        f"{{\"director\": \"Christopher Nolan\", \"start_year\": 2000, \"end_year\": 2010}}"
        f"Example for 'comedy movies from the 90s': "
        f"{{\"director\": null, \"start_year\": 1990, \"end_year\": 1999}}"
        f"Return ONLY the JSON object, do not add any extra text or explanations. "
        f"Here is the user's query: '{query}'"
    )

    try:
        print("Calling Gemini API to parse query for specific details...")
        payload = {
            "contents": [{"parts": [{"text": parsing_prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "topP": 0.1,
                "candidateCount": 1
            }
        }
        
        response = requests.post(API_URL, json=payload)
        response.raise_for_status()
        
        result = response.json()
        json_text = result['candidates'][0]['content']['parts'][0]['text']
        
        match = re.search(r'\{.*\}', json_text, re.DOTALL)
        if match:
            parsed_data = json.loads(match.group(0))
            
            # Check if any specific information was actually found
            if parsed_data.get('director') or parsed_data.get('start_year') or parsed_data.get('end_year'):
                # Add keywords and return the structured data
                filtered_keywords = [word.lower() for word in re.findall(r'\b\w+\b', query) if word.lower() not in STOP_WORDS]
                parsed_data['keywords'] = filtered_keywords
                parsed_data['movie_titles'] = []
                return parsed_data
    
    except Exception as e:
        print(f"An error occurred during specific query parsing: {e}", file=sys.stderr)
        traceback.print_exc()

    # Fallback logic: if no specific details were found or if parsing failed,
    # switch to generating a list of movie titles
    titles_prompt = (
        f"Based on the following description, provide a list of 10 movie titles that match. "
        f"Return ONLY a JSON object with a single key 'movie_titles' which holds an array of strings. "
        f"Do NOT include any extra text or formatting besides the JSON object.\n\n"
        f"Description: '{query}'"
        f"Example output: {{\"movie_titles\": [\"Movie 1\", \"Movie 2\", \"Movie 3\"]}}"
    )
    
    try:
        print("Falling back to Gemini API to generate movie titles...")
        payload = {
            "contents": [{"parts": [{"text": titles_prompt}]}],
            "generationConfig": {
                "temperature": 0.5,
                "topP": 0.9,
                "candidateCount": 1
            }
        }

        response = requests.post(API_URL, json=payload)
        response.raise_for_status()

        result = response.json()
        json_text = result['candidates'][0]['content']['parts'][0]['text']
        
        parsed_titles = json.loads(json_text)
        
        if 'movie_titles' in parsed_titles and isinstance(parsed_titles['movie_titles'], list):
            return {
                "keywords": [],
                "director": None,
                "start_year": None,
                "end_year": None,
                "movie_titles": parsed_titles['movie_titles']
            }

    except json.JSONDecodeError as e:
        # If JSON parsing fails, try to extract titles with a regex
        print(f"JSONDecodeError occurred. Attempting to parse titles with regex.", file=sys.stderr)
        
        # Regex to find quoted strings that look like movie titles
        title_matches = re.findall(r'\"([^\"]*)\"', json_text)
        if title_matches:
            print(f"Successfully extracted titles via regex: {title_matches}")
            return {
                "keywords": [],
                "director": None,
                "start_year": None,
                "end_year": None,
                "movie_titles": title_matches
            }
        else:
            print("Regex also failed to find titles.", file=sys.stderr)
            traceback.print_exc()
            
    except Exception as e:
        # Handle any other exceptions
        print(f"An unexpected error occurred during title generation fallback: {e}", file=sys.stderr)
        traceback.print_exc()
        
    # Final fallback if everything fails
    return {
        "keywords": [query],
        "director": None,
        "start_year": None,
        "end_year": None,
        "movie_titles": []
    }