import sqlite3
import json

# A simple class to hold movie data
class Movie:
    def __init__(self, id, title, overview, release_date, poster_path, genre_ids, popularity):
        self.id = id
        self.title = title
        self.overview = overview
        self.release_date = release_date
        self.poster_path = poster_path
        self.genre_ids = genre_ids
        self.popularity = popularity

def setup_sqlite_db():
    """Sets up the SQLite database and creates the movies table."""
    try:
        conn = sqlite3.connect("movies.db")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                overview TEXT,
                release_date TEXT,
                poster_path TEXT,
                genre_ids TEXT,
                popularity REAL
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        if conn:
            conn.close()

def save_movies_to_sqlite(movies_data: list, genre_map: dict):
    """Saves movie metadata to the SQLite database."""
    try:
        conn = sqlite3.connect("movies.db")
        cursor = conn.cursor()
        
        # This will be used to check for existing IDs
        existing_ids = get_existing_movie_ids([movie.get('id') for movie in movies_data])
        
        insert_query = """
            INSERT OR IGNORE INTO movies (
                id, title, overview, release_date, poster_path, genre_ids, popularity
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        for movie in movies_data:
            movie_id = movie.get("id")
            if movie_id not in existing_ids:
                genre_ids_json = json.dumps(movie.get("genre_ids", []))
                cursor.execute(insert_query, (
                    movie_id,
                    movie.get("title"),
                    movie.get("overview"),
                    movie.get("release_date"),
                    movie.get("poster_path"),
                    genre_ids_json,
                    movie.get("popularity")
                ))
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        if conn:
            conn.close()

def get_movies_by_ids_from_sqlite(movie_ids: list) -> list:
    """Fetches movie data from SQLite based on a list of IDs."""
    if not movie_ids:
        return []

    try:
        conn = sqlite3.connect("movies.db")
        cursor = conn.cursor()
        
        placeholders = ','.join(['?'] * len(movie_ids))
        select_query = f"SELECT * FROM movies WHERE id IN ({placeholders})"
        
        cursor.execute(select_query, movie_ids)
        rows = cursor.fetchall()
        
        movies = []
        for row in rows:
            movies.append(Movie(*row))
        return movies
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_existing_movie_ids(movie_ids: list) -> set:
    """Checks for existing movie IDs in the database."""
    if not movie_ids:
        return set()
    
    try:
        conn = sqlite3.connect("movies.db")
        cursor = conn.cursor()
        
        placeholders = ','.join(['?'] * len(movie_ids))
        select_query = f"SELECT id FROM movies WHERE id IN ({placeholders})"
        
        cursor.execute(select_query, movie_ids)
        rows = cursor.fetchall()
        
        return {row[0] for row in rows}
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return set()
    finally:
        if conn:
            conn.close()