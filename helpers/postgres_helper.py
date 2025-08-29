# ==============================================================================
# helpers/postgres_helper.py
# Manages PostgreSQL database connections and schema.
# ==============================================================================
import os
import sys
import psycopg2
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Movie:
    id: int  # Зробив Optional, щоб дозволити None для нових об'єктів
    title: str = None
    overview: str = None
    popularity: float = None
    vote_average: float = None
    vote_count: int = None
    release_date: str = None
    poster_path: str = None
    adult: bool = False
    backdrop_path: str = None
    original_language: str = None
    original_title: str = None
    video: bool = False
    director_name: str = None
    genre_ids: list[int] = field(default_factory=list)
    # Виправлення: додаємо tmdb_id
    tmdb_id: Optional[int] = field(default=None)

    def to_dict(self):
        return self.__dict__

class PostgresHelper:
    """Helper class to manage PostgreSQL database operations."""
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv("POSTGRES_DB"),
            'user': os.getenv("POSTGRES_USER"),
            'password': os.getenv("POSTGRES_PASSWORD"),
            'host': os.getenv("POSTGRES_HOST"),
            'port': os.getenv("POSTGRES_PORT")
        }

    def get_connection(self):
        """Establishes and returns a connection to the database."""
        try:
            conn = psycopg2.connect(**self.db_params)
            return conn
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}", file=sys.stderr)
            sys.exit(1)

    def init_database(self):
        """Initializes the PostgreSQL database schema if it doesn't exist."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS directors (
                            id SERIAL PRIMARY KEY,
                            name TEXT UNIQUE NOT NULL
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS movies (
                            tmdb_id INT PRIMARY KEY,
                            title TEXT NOT NULL,
                            overview TEXT,
                            popularity FLOAT,
                            vote_average FLOAT,
                            vote_count INT,
                            release_date DATE,
                            poster_path TEXT,
                            backdrop_path TEXT,
                            original_language TEXT,
                            original_title TEXT,
                            video BOOLEAN,
                            adult BOOLEAN
                        );
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS movie_directors (
                            movie_id INT REFERENCES movies (tmdb_id) ON DELETE CASCADE,
                            director_id INT REFERENCES directors (id) ON DELETE CASCADE,
                            PRIMARY KEY (movie_id, director_id)
                        );
                    """)
                    conn.commit()
            print("Database schema initialized successfully.")
        except Exception as e:
            print(f"Error initializing database schema: {e}", file=sys.stderr)
            raise e

    def movie_exists_in_db(self, tmdb_id: int) -> bool:
        """Checks if a movie with the given TMDb ID already exists in the database."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM movies WHERE tmdb_id = %s", (tmdb_id,))
                    return cur.fetchone() is not None
        except Exception as e:
            print(f"Error checking for movie existence: {e}", file=sys.stderr)
            return False

    def save_movies_to_db(self, movies: List[Movie]):
        """Saves movies and their director associations to the database."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for movie in movies:
                        # Ми використовуємо tmdb_id як первинний ключ
                        cur.execute("""
                            INSERT INTO movies (tmdb_id, title, overview, popularity, vote_average,
                            vote_count, release_date, poster_path, backdrop_path, original_language,
                            original_title, video, adult) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (tmdb_id) DO UPDATE SET
                            title = EXCLUDED.title, overview = EXCLUDED.overview, popularity = EXCLUDED.popularity,
                            vote_average = EXCLUDED.vote_average, vote_count = EXCLUDED.vote_count,
                            release_date = EXCLUDED.release_date, poster_path = EXCLUDED.poster_path,
                            backdrop_path = EXCLUDED.backdrop_path, original_language = EXCLUDED.original_language,
                            original_title = EXCLUDED.original_title, video = EXCLUDED.video, adult = EXCLUDED.adult;
                        """, (movie.tmdb_id, movie.title, movie.overview, movie.popularity, movie.vote_average,
                            movie.vote_count, movie.release_date, movie.poster_path, movie.backdrop_path,
                            movie.original_language, movie.original_title, movie.video, movie.adult))
                        
                        if movie.director_name:
                            cur.execute(
                                "INSERT INTO directors (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id;",
                                (movie.director_name,)
                            )
                            director_id = cur.fetchone()[0]
                            cur.execute(
                                "INSERT INTO movie_directors (movie_id, director_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                                (movie.tmdb_id, director_id)
                            )
                    conn.commit()
            print(f"Ingested {len(movies)} movies and directors to DB.")
        except Exception as e:
            print(f"Error saving movies to database: {e}", file=sys.stderr)
            raise e

    def clear_all_tables(self):
        """Clears all data from the tables."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE movie_directors, directors, movies RESTART IDENTITY CASCADE;")
                    conn.commit()
            print("All database tables have been cleared.")
        except Exception as e:
            print(f"Error clearing tables: {e}", file=sys.stderr)
            raise e

    def get_movies_by_ids_from_db(self, movie_ids: List[int]) -> List[Movie]:
        """Fetches movies by their TMDb IDs from the database."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    placeholders = ', '.join(['%s'] * len(movie_ids))
                    query = f"SELECT * FROM movies WHERE tmdb_id IN ({placeholders});"
                    cur.execute(query, tuple(movie_ids))
                    movies_data = cur.fetchall()
            
            if not movies_data:
                return []
            
            movies = []
            for row in movies_data:
                movies.append(Movie(
                    id=row[0], # Змінив з id на tmdb_id,
                    tmdb_id=row[0], # Змінив з id на tmdb_id
                    title=row[1], overview=row[2], popularity=row[3],
                    vote_average=row[4], vote_count=row[5], release_date=str(row[6]),
                    poster_path=row[7], backdrop_path=row[8], original_language=row[9],
                    original_title=row[10], video=row[11], adult=row[12], director_name=None
                ))
            return movies
            
        except Exception as e:
            print(f"Error fetching movies from DB by ID: {e}", file=sys.stderr)
            return []