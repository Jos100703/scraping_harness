import asyncio
import logging
from datetime import datetime
from typing import Optional

from pymongo import UpdateOne
from tqdm import tqdm

from .connection import MongoConnection
from .models import Movie


class Pipeline:
    collection_name: str = "movies"
    pipeline_name: str = "unnamed"    # subclass overrides; drives _meta.status key
    bulk_size: int = 100

    def __init__(self, connection: MongoConnection = None):
        self.conn = connection or MongoConnection()
        self.collection = self.conn.get_collection(self.collection_name)

    def query(self) -> dict:
        """Default: find docs where _meta.status.<pipeline_name> is absent."""
        return {f"_meta.status.{self.pipeline_name}": {"$exists": False}}

    def process(self, movie: Movie) -> Optional[dict]:
        """Process one movie. Return dict of fields to $set, or None to skip."""
        raise NotImplementedError

    def _flush(self, ops: list):
        if ops:
            self.collection.bulk_write(ops, ordered=False)

    def run(self, limit: int = 0):
        cursor = self.collection.find(self.query())
        if limit > 0:
            cursor = cursor.limit(limit)

        total = self.collection.count_documents(self.query())
        pbar = tqdm(total=min(total, limit) if limit else total, desc=self.pipeline_name)

        ops = []
        for doc in cursor:
            try:
                movie = Movie(**doc)
                updates = self.process(movie)
                status_val = "done" if updates is not None else "skipped"
                set_dict = updates or {}
                set_dict[f"_meta.status.{self.pipeline_name}"] = status_val
                set_dict["_meta.updated_at"] = datetime.now()
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": set_dict}))
                if len(ops) >= self.bulk_size:
                    self._flush(ops)
                    ops.clear()
            except Exception as e:
                logging.error(f"Error on {doc.get('_id')}: {e}")
            pbar.update(1)

        self._flush(ops)
        pbar.close()


class AsyncPipeline(Pipeline):
    max_concurrency: int = 10
    chunk_size: int = 50

    async def process_batch(self, movies: list[Movie]) -> list[Optional[dict]]:
        """Override this. Use @async_wrapper internally for concurrent I/O."""
        return [self.process(m) for m in movies]

    async def run_async(self, limit: int = 0):
        total = self.collection.count_documents(self.query())
        pbar = tqdm(total=min(total, limit) if limit else total, desc=self.pipeline_name)
        processed = 0

        while True:
            fetch = min(self.chunk_size, limit - processed) if limit else self.chunk_size
            docs = list(self.collection.find(self.query()).limit(fetch))
            if not docs:
                break

            movies = [Movie(**doc) for doc in docs]
            results = await self.process_batch(movies)

            ops = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": {
                        **(updates or {}),
                        f"_meta.status.{self.pipeline_name}": "done" if updates is not None else "skipped",
                        "_meta.updated_at": datetime.now(),
                    }},
                )
                for doc, updates in zip(docs, results)
            ]
            self._flush(ops)

            processed += len(docs)
            pbar.update(len(docs))
            if limit and processed >= limit:
                break

        pbar.close()

    def run(self, limit: int = 0):
        asyncio.run(self.run_async(limit))
