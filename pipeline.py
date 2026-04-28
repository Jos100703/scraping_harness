import asyncio
import logging
import math
from datetime import datetime, timezone
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


class ScraperPipeline:
    """Abstract base for scraper pipelines that iterate over pre-computed
    combinations, call an external API, and bulk-write results to MongoDB.

    Subclass and override the seven abstract methods. run() provides the
    outer batch loop with tqdm and bulk_write automatically.
    """

    movies_collection_name: str = "movies"
    batch_size: int = 20
    concurrency: int = 20
    scraper_name: str = "unnamed_scraper"

    def __init__(self, connection: MongoConnection = None):
        self.conn = connection or MongoConnection()
        self.movies_col = self.conn.get_collection(self.movies_collection_name)

    # ── Override these ──────────────────────────────────────────────────────

    def load_combinations(self):
        """Return all combinations to iterate over (list or DataFrame)."""
        raise NotImplementedError

    def set_batch(self, batch):
        """Push the batch slice into shared scraper state."""
        raise NotImplementedError

    def load_state(self):
        """Restore pagination/cursor state for the current batch."""
        raise NotImplementedError

    def has_more(self) -> bool:
        """True while there are remaining pages/items in the current batch."""
        raise NotImplementedError

    async def scrape(self) -> list:
        """One async scrape iteration. Return raw results."""
        raise NotImplementedError

    def build_upserts(self, results: list) -> list:
        """Convert raw scrape results into pymongo UpdateOne operations."""
        raise NotImplementedError

    def update_state(self, results: list):
        """Persist cursor/state advancement after one iteration."""
        raise NotImplementedError

    def post_write(self, results: list):
        """Optional hook for secondary collection writes after the primary bulk_write."""
        pass

    # ── Checkpoint helpers ──────────────────────────────────────────────────

    def _load_checkpoint(self) -> int:
        """Return the last completed batch start-index for today, or -1."""
        col = self.conn.get_collection("scrape_checkpoints")
        doc = col.find_one({"scraper_name": self.scraper_name, "run_date": self._run_date})
        return doc["last_batch_idx"] if doc else -1

    def _save_checkpoint(self, batch_idx: int) -> None:
        """Record that the batch starting at batch_idx has completed."""
        col = self.conn.get_collection("scrape_checkpoints")
        col.update_one(
            {"scraper_name": self.scraper_name, "run_date": self._run_date},
            {"$set": {"last_batch_idx": batch_idx}},
            upsert=True,
        )

    def _clear_checkpoint(self) -> None:
        """Remove the checkpoint document after a successful run."""
        col = self.conn.get_collection("scrape_checkpoints")
        col.delete_one({"scraper_name": self.scraper_name, "run_date": self._run_date})

    # ── Main loop ───────────────────────────────────────────────────────────

    def run(self):
        combinations = self.load_combinations()
        n = len(combinations)
        num_batches = math.ceil(n / self.batch_size)

        self._run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        resume_after = self._load_checkpoint()

        for i in tqdm(range(0, n, self.batch_size),
                      total=num_batches, desc=self.scraper_name):
            if i <= resume_after:
                continue

            self.set_batch(combinations[i : i + self.batch_size])
            self.load_state()

            while self.has_more():
                results = asyncio.run(self.scrape())
                ops = self.build_upserts(results)
                if ops:
                    self.movies_col.bulk_write(ops, ordered=False)
                self.post_write(results)
                self.update_state(results)

            self._save_checkpoint(i)

        self._clear_checkpoint()
