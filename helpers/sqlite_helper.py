import sqlite3
import json
import traceback
import sys
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any


DATABASE_NAME = "movies.db"

@dataclass
class Movie:
    """A data class to represent a movie with key attributes."""
    # Core fields
    id: int
    title: str
    overview: str
    popularity: float
    vote_average: float
    vote_count: int
    
    # Optional fields (can be None)
    release_date: str | None = None
    poster_path: str | None = None
    genre_ids: List[int] = field(default_factory=list)
    
    # Additional fields from TMDB API to prevent TypeError
    adult: bool = False
    backdrop_path: str | None = None
    original_language: str | None = None
    original_title: str | None = None
    video: bool = False

    def to_dict(self):
        """Converts the dataclass object into a dictionary."""
        return asdict(self)


def setup_sqlite_db():
    """Initializes the SQLite database and creates the movies table."""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                overview TEXT,
                poster_path TEXT,
                release_date TEXT,
                vote_average REAL,
                genre_names TEXT,
                popularity REAL,
                original_language TEXT,
                adult INTEGER
            );
        """)
        conn.commit()
        conn.close()
        print("SQLite database setup complete.")
    except sqlite3.Error as e:
        print(f"SQLite error during setup: {e}", file=sys.stderr)
        traceback.print_exc()

def save_movies_to_sqlite(movies: list[Movie], genre_map: dict = None):
    """Saves a list of Movie objects to the SQLite database."""
    if not movies:
        print("No movies to save to SQLite.", file=sys.stderr)
        return

    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Prepare data for insertion
        movies_data = []
        for movie in movies:
            genre_names = ", ".join([genre_map.get(str(g_id), "Unknown") for g_id in movie.genre_ids]) if genre_map and movie.genre_ids else ""
            movies_data.append((
                movie.id,
                movie.title,
                movie.overview,
                movie.poster_path,
                movie.release_date,
                movie.vote_average,
                genre_names,
                movie.popularity,
                movie.original_language,
                1 if movie.adult else 0
            ))

        cursor.executemany("""
            INSERT OR IGNORE INTO movies (
                id, title, overview, poster_path, release_date,
                vote_average, genre_names, popularity, original_language, adult
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, movies_data)
        
        conn.commit()
        conn.close()
        print(f"Successfully saved {cursor.rowcount} movies to SQLite.")
    except sqlite3.Error as e:
        print(f"SQLite error during save: {e}", file=sys.stderr)
        traceback.print_exc()


    def to_dict(self):
        """Converts the dataclass object into a dictionary."""
        return asdict(self)

def get_movies_by_ids_from_sqlite(movie_ids: list[int]) -> list[Movie]:
    """Retrieves a list of movies from SQLite by their IDs."""
    if not movie_ids:
        return []

    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Use a prepared statement with placeholders for security
        placeholders = ','.join('?' for _ in movie_ids)
        cursor.execute(f"SELECT * FROM movies WHERE id IN ({placeholders});", movie_ids)
        
        rows = cursor.fetchall()
        
        # Convert the retrieved rows back into Movie objects
        movies = []
        for row in rows:
            movies.append(Movie(
                id=row[0],
                title=row[1],
                overview=row[2],
                poster_path=row[3],
                release_date=row[4],
                vote_average=row[5],
                # FIX: Add vote_count from the database. It is not stored in the table.
                # Since we don't store it, we can pass a placeholder value.
                vote_count=0,
                genre_ids=[], # We don't store genre IDs directly, so we leave this empty
                popularity=row[7],
                original_language=row[8],
                adult=bool(row[9])
            ))
        
        conn.close()
        return movies
    except sqlite3.Error as e:
        print(f"SQLite error during ID retrieval: {e}", file=sys.stderr)
        traceback.print_exc()
        return []

def get_movie_by_title_from_sqlite(title: str) -> Movie | None:
    """Retrieves a single movie from SQLite by its title."""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Use a prepared statement to prevent SQL injection
        cursor.execute("SELECT * FROM movies WHERE title = ? COLLATE NOCASE;", (title,))
        
        row = cursor.fetchone()
        conn.close()

        if row:
            # Convert the row into a Movie object
            return Movie(
                id=row[0],
                title=row[1],
                overview=row[2],
                poster_path=row[3],
                release_date=row[4],
                vote_average=row[5],
                # FIX: Add vote_count from the database. It is not stored.
                vote_count=0,
                genre_ids=[],
                popularity=row[7],
                original_language=row[8],
                adult=bool(row[9])
            )
        else:
            return None
    except sqlite3.Error as e:
        print(f"SQLite error during title retrieval: {e}", file=sys.stderr)
        traceback.print_exc()
        return None

def get_existing_movie_ids() -> set:
    """Returns a set of all existing movie IDs in the database."""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM movies;")
        ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        return ids
    except sqlite3.Error as e:
        print(f"SQLite error during ID retrieval: {e}", file=sys.stderr)
        return set()