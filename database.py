# -*- coding: utf-8 -*-
import sqlite3
import json
import sys

class MovieModel:
    """
    Data model to represent a movie from the TMDB API.
    """
    def __init__(self, id: int, title: str, overview: str, release_date: str,
                 vote_average: float, popularity: float, original_language: str,
                 genres: list[str]):
        self.id = id
        self.title = title
        self.overview = overview
        self.release_date = release_date
        self.vote_average = vote_average
        self.popularity = popularity
        self.original_language = original_language
        self.genres = genres

def save_movies_to_db(movies_data: list, genre_map: dict):
    """
    Connects to an SQLite database and saves movie data with genre names.
    """
    DB_NAME = "movies.db"
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        # Create the movies table if it doesn't already exist.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                release_date TEXT,
                overview TEXT,
                vote_average REAL,
                popularity REAL,
                original_language TEXT,
                genre_ids TEXT,
                genre_names TEXT,
                embedding BLOB
            )
        """)
        conn.commit()

        # Prepare data for insertion
        movies_to_insert = []
        for movie in movies_data:
            movie_id = movie.get("id")
            title = movie.get("title", "Title unknown")
            release_date = movie.get("release_date", "Release date unknown")
            overview = movie.get("overview", "Overview not available")
            vote_average = movie.get("vote_average", None)
            popularity = movie.get("popularity", None)
            original_language = movie.get("original_language", "N/A")
            
            # Convert genre IDs to a string for storage
            genre_ids_list = movie.get("genre_ids", [])
            genre_ids = json.dumps(genre_ids_list)
            
            # Convert genre IDs to names using the mapping
            genre_names_list = [genre_map.get(gid, "Unknown") for gid in genre_ids_list]
            genre_names = ", ".join(genre_names_list)
            
            movies_to_insert.append((
                movie_id, title, release_date, overview, vote_average,
                popularity, original_language, genre_ids, genre_names, None # Embedding will be added later
            ))

        # Use INSERT OR IGNORE to prevent overwriting existing data
        cursor.executemany("""
            INSERT OR IGNORE INTO movies (id, title, release_date, overview, vote_average, popularity, original_language, genre_ids, genre_names, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, movies_to_insert)

        conn.commit()
        print(f"Successfully saved {cursor.rowcount} new movies to '{DB_NAME}'.")

    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()

def get_movies_from_db() -> list[MovieModel]:
    """
    Retrieves all movies from the SQLite database and returns a list of MovieModel objects.
    """
    DB_NAME = "movies.db"
    movies = []
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, title, overview, release_date, vote_average, popularity, original_language, genre_names FROM movies")
        rows = cursor.fetchall()
        
        for row in rows:
            movie_id, title, overview, release_date, vote_average, popularity, original_language, genre_names_str = row
            
            # Convert genre string back to a list
            genres_list = genre_names_str.split(", ")
            
            movie_model = MovieModel(
                id=movie_id,
                title=title,
                overview=overview,
                release_date=release_date,
                vote_average=vote_average,
                popularity=popularity,
                original_language=original_language,
                genres=genres_list
            )
            movies.append(movie_model)

    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
            
    return movies

