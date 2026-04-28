import json
import os
import re
from pathlib import Path

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

DEFAULT_URI = "mongodb://localhost:27017"
DEFAULT_DB  = "movie_politics"


class MongoConnection:
    def __init__(self, uri=None, db_name=None):
        self.uri = uri or os.getenv("MONGO_URI", DEFAULT_URI)
        self.db_name = db_name or os.getenv("MONGO_DB", DEFAULT_DB)
        self._client = None
        self._db = None

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(self.uri)
        return self._client

    @property
    def db(self) -> Database:
        if self._db is None:
            self._db = self.client[self.db_name]
        return self._db

    def get_collection(self, name="movies") -> Collection:
        return self.db[name]

    def ensure_indexes(self):
        movies = self.get_collection("movies")
        movies.create_index("imdb_id", unique=True, sparse=True)
        movies.create_index("source_entries.internal_id")
        movies.create_index([("_meta.status", 1)])

    @classmethod
    def export_to_json(cls, imdb_ids: list[str], output_dir: str = ".", collection_name: str = "movies") -> int:
        """Fetch movies by IMDb ID and write each to <output_dir>/<imdb_id>_<title>.json.

        Returns the number of files written.
        """
        from .models import Movie

        connection = cls()
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        collection = connection.get_collection(collection_name)
        docs = collection.find({"imdb_id": {"$in": list(imdb_ids)}})

        exported = 0
        for doc in docs:
            imdb_id = doc.get("imdb_id", "unknown")
            title = doc.get("title") or "untitled"
            safe_title = re.sub(r"[^\w\s-]", "", title).strip().replace(" ", "_")
            filename = f"{imdb_id}_{safe_title}.json"

            movie = Movie(**doc)
            with open(out / filename, "w", encoding="utf-8") as f:
                json.dump(movie.model_dump(mode="json", exclude_none=True), f, indent=2, ensure_ascii=False)
            exported += 1

        return exported

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
