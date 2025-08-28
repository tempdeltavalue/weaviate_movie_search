import psycopg2
import os
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any

@dataclass
class Movie:
    """A data class to represent a movie with key attributes."""
    id: int
    title: str
    overview: str
    popularity: float
    vote_average: float
    vote_count: int
    
    release_date: str | None = None
    poster_path: str | None = None
    genre_ids: List[int] = field(default_factory=list)
    
    adult: bool = False
    backdrop_path: str | None = None
    original_language: str | None = None
    original_title: str | None = None
    video: bool = False

    def to_dict(self):
        return asdict(self)


class PostgresHelper:
    """Helper class for managing PostgreSQL database connections."""

    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="movie_db",
            user="tempdeltauser",
            password=os.getenv("POSTGRE_PASSWORD"),
            host="localhost",
            port="5432"
        )
        self.conn.autocommit = False

    def init_database(self):
        """Initializes the PostgreSQL database schema if it does not exist."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        id INT PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        overview TEXT,
                        poster_path VARCHAR(255),
                        release_date VARCHAR(255),
                        vote_average REAL,
                        genre_names TEXT,
                        popularity REAL,
                        original_language VARCHAR(255),
                        adult BOOLEAN
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS keywords (
                        movie_id INT,
                        keyword TEXT,
                        PRIMARY KEY (movie_id, keyword),
                        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
                    );
                """)
            self.conn.commit()
            print("PostgreSQL database setup complete.")
        except psycopg2.DatabaseError as e:
            print(f"PostgreSQL error during setup: {e}")
            self.conn.rollback()

    def save_movies_to_db(self, movies: list[Movie], genre_map: dict = None):
        """Saves a list of Movie objects to the PostgreSQL database."""
        if not movies:
            return

        try:
            with self.conn.cursor() as cur:
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
                        movie.adult
                    ))

                cur.executemany("""
                    INSERT INTO movies AS m (
                        id, title, overview, poster_path, release_date,
                        vote_average, genre_names, popularity, original_language, adult
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        overview = EXCLUDED.overview,
                        poster_path = EXCLUDED.poster_path,
                        release_date = EXCLUDED.release_date,
                        vote_average = EXCLUDED.vote_average,
                        genre_names = EXCLUDED.genre_names,
                        popularity = EXCLUDED.popularity,
                        original_language = EXCLUDED.original_language,
                        adult = EXCLUDED.adult;
                """, movies_data)
            self.conn.commit()
            print(f"Successfully saved {len(movies_data)} movies to PostgreSQL.")
        except psycopg2.DatabaseError as e:
            print(f"PostgreSQL error during save: {e}")
            self.conn.rollback()

    def get_movies_by_ids_from_db(self, movie_ids: list[int]) -> list[Movie]:
        """Retrieves a list of movies from PostgreSQL by their IDs."""
        if not movie_ids:
            return []
        
        try:
            with self.conn.cursor() as cur:
                placeholders = ','.join(['%s'] * len(movie_ids))
                cur.execute(f"SELECT * FROM movies WHERE id IN ({placeholders});", movie_ids)
                rows = cur.fetchall()
                
                movies = []
                for row in rows:
                    movies.append(Movie(
                        id=row[0],
                        title=row[1],
                        overview=row[2],
                        poster_path=row[3],
                        release_date=row[4],
                        vote_average=row[5],
                        vote_count=0,
                        genre_ids=[],
                        popularity=row[7],
                        original_language=row[8],
                        adult=row[9]
                    ))
                return movies
        except psycopg2.DatabaseError as e:
            print(f"PostgreSQL error during ID retrieval: {e}")
            self.conn.rollback()
            return []

    def get_movie_by_title_from_db(self, title: str) -> Movie | None:
        """Retrieves a single movie from PostgreSQL by its title."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM movies WHERE lower(title) = lower(%s);", (title,))
                row = cur.fetchone()

                if row:
                    return Movie(
                        id=row[0],
                        title=row[1],
                        overview=row[2],
                        poster_path=row[3],
                        release_date=row[4],
                        vote_average=row[5],
                        vote_count=0,
                        genre_ids=[],
                        popularity=row[7],
                        original_language=row[8],
                        adult=row[9]
                    )
                else:
                    return None
        except psycopg2.DatabaseError as e:
            print(f"PostgreSQL error during title retrieval: {e}")
            self.conn.rollback()
            return None

    def get_existing_movie_ids(self) -> set:
        """Returns a set of all existing movie IDs in the database."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM movies;")
                ids = {row[0] for row in cur.fetchall()}
                return ids
        except psycopg2.DatabaseError as e:
            print(f"PostgreSQL error during ID retrieval: {e}")
            self.conn.rollback()
            return set()
    
    def close(self):
        if self.conn:
            self.conn.close()