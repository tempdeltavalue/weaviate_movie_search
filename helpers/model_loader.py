# ==============================================================================
# Helper functions for NLP models, embeddings, and query generation.
# This file is intended to be used as a module in the main application.
# ==============================================================================
import sys
import os 
import torch
from sentence_transformers import SentenceTransformer
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

import random 

# ==============================================================================
# Global Model Configuration
# ==============================================================================
# Use a tiny and fast model for demonstration
# You can set this environment variable to a different model if you want
MODEL_NAME = os.getenv("HF_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MODEL = None

# Set this flag to False to use the dummy function,
# or True to use the real NER model for movie title extraction.
# This is a key configuration for the `generate_search_query` function.
use_real_model = False

# Global model for query generation
_query_generator_model = None

# ==============================================================================
# Functions for Text Embeddings (for Semantic Search)
# ==============================================================================
def load_model():
    """Loads the sentence-transformer model once."""
    global MODEL
    if MODEL is None:
        print(f"Loading model: {MODEL_NAME}...")
        try:
            # Check for GPU availability
            device = "cuda" if torch.cuda.is_available() else "cpu"
            MODEL = SentenceTransformer(MODEL_NAME, device=device)
            print("Model loaded successfully.")
            print(f"Model is running on device: {device}")
        except Exception as e:
            print(f"Failed to load model: {e}", file=sys.stderr)
            # Fallback to a simple error
            MODEL = None
    return MODEL

def get_text_embedding(text: str) -> list[float]:
    """Generates an embedding for a single text using the loaded model."""
    model = load_model()
    if model:
        # The encode method handles single texts as well, but we'll use a list
        # for consistency with the batch function.
        embedding = model.encode([text])
        return embedding[0].tolist()
    else:
        # Fallback to a random vector if the model fails to load
        return [random.uniform(-1, 1) for _ in range(384)] # Note: The model's dimension is 384

def get_text_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Generates a batch of embeddings for a list of texts using the loaded model."""
    model = load_model()
    if model:
        embeddings = model.encode(texts, convert_to_tensor=True)
        return embeddings.tolist()
    else:
        # Fallback to random vectors if the model fails to load
        return [[random.uniform(-1, 1) for _ in range(384)] for _ in texts] # Note: The model's dimension is 384

# ==============================================================================
# Functions for Query Generation
# ==============================================================================
def get_query_generator():
    """
    Loads a fine-tuned Hugging Face model for Named Entity Recognition (NER).
    This model is specifically trained to identify movie titles.
    """
    global _query_generator_model
    if _query_generator_model is None:
        print("Loading fine-tuned movie title NER model...")
        model_name = "thatdramebaazguy/roberta-base-MITmovie"
        try:
            # Use the 'token-classification' pipeline for NER
            _query_generator_model = pipeline("token-classification", model=model_name)
            print("Fine-tuned NER model loaded successfully.")
        except Exception as e:
            print(f"Failed to load fine-tuned NER model: {e}", file=sys.stderr)
            _query_generator_model = None
    return _query_generator_model

def generate_search_query_from_description(user_input: str) -> str:
    """
    Generates a search query based on the user_input using a real NER model.
    This is an internal helper function.
    """
    model_pipeline = get_query_generator()
    if not model_pipeline:
        print("Model pipeline not available. Returning original query.")
        return user_input
    
    try:
        ner_results = model_pipeline(user_input)
        
        movie_title_parts = []
        for result in ner_results:
            if result['entity_group'] == 'TITLE':
                movie_title_parts.append(result['word'])
        
        if movie_title_parts:
            movie_title = "".join(movie_title_parts).replace(" ", " ").strip()
            generated_query = f"{movie_title} movie"
            print(f"Identified movie title: '{movie_title}'")
            print(f"Generated query: '{generated_query}'")
            return generated_query
        else:
            print("No movie title found. Returning original query.")
            return user_input
    except Exception as e:
        print(f"An error occurred during query generation: {e}", file=sys.stderr)
        return user_input

def get_dummy_query(user_input: str) -> str:
    """
    Returns the hardcoded string "love" as a placeholder.
    This is an internal helper function.
    """
    print("Using dummy function to return 'love' as the search query.")
    return "Space"

def generate_search_query(user_input: str) -> str:
    """
    The main function for generating a search query based on user input.
    It uses either a real NLP model or a dummy function based on the
    `use_real_model` flag.
    """
    if use_real_model:
        return generate_search_query_from_description(user_input)
    else:
        return get_dummy_query(user_input)

# ==============================================================================
# Main execution block for testing the NLP helper
# ==============================================================================
def main():
    """A test function for the NLP helper."""
    # Test with the real model
    embedding_real = get_text_embedding("A film about a famous pirate of the caribbean")
    print(f"Embedding from real model: {embedding_real[:5]}...") # Print first 5 elements
    
    # Test with the dummy query function
    dummy_query = generate_search_query("Any description here")
    print(f"Dummy generated query: '{dummy_query}'")

if __name__ == "__main__":
    main()
