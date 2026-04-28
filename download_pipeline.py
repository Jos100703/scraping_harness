"""Reusable download-pipeline harness.

Extracts the shared argparse + chunk-loop + status-tracking boilerplate from
``04_retrieve_imdb_metadata.py`` so that subtitle / profile-pic / movie-still
scripts can each subclass ``DownloadPipeline`` and only implement the
domain-specific logic.
"""

import argparse
import json
import logging
import math
from abc import ABC, abstractmethod
from datetime import datetime

from pymongo import UpdateOne
from tqdm import tqdm

from .connection import MongoConnection
from .utils import cleanup


class DownloadPipeline(ABC):
    """Base class for chunk-based MongoDB download pipelines.

    Subclass must set the class-level config attributes and implement
    ``process_chunk`` and ``build_update``.
    """

    # ── Class-level config (subclass overrides) ────────────────────────────

    PIPELINE_NAME: str = ""          # drives ``_meta.status.{PIPELINE_NAME}``
    COLLECTION: str = "nfx_survey"   # MongoDB collection name
    ALL_STEPS: set[str] = set()      # e.g. {"download", "extract"}
    DEFAULT_CHUNK_SIZE: int = 10
    BASE_FILTER: dict = {}           # extra query filter merged into every query

    # ── Abstract methods ───────────────────────────────────────────────────

    @abstractmethod
    def process_chunk(self, docs: list[dict], steps: set[str]) -> dict:
        """Do the actual download / processing work for *docs*.

        *steps* is the resolved set of step names to execute (may be a
        subset when ``--only`` is used).

        Return an arbitrary results dict that ``build_update`` can consume.
        """

    @abstractmethod
    def build_update(self, doc: dict, results: dict, only: set[str] | None) -> dict:
        """Map *results* to a ``$set`` field dict for one *doc*.

        *only*: the raw ``--only`` set (``None`` means all steps).  Use it to
        decide which fields to write.
        """

    # ── Optional hooks ─────────────────────────────────────────────────────

    def setup(self, args: argparse.Namespace) -> None:
        """Called once before the main loop.  Override to init sessions, etc."""

    def teardown(self) -> None:
        """Called after the main loop.  Override to close sessions, etc."""

    # ── Provided by base class ─────────────────────────────────────────────

    def make_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description=f"Download pipeline: {self.PIPELINE_NAME}",
        )
        parser.add_argument("--limit", type=int, default=0,
                            help="Max documents to process (0 = all)")
        parser.add_argument("--chunk-size", type=int, default=self.DEFAULT_CHUNK_SIZE)
        parser.add_argument("--rerun", action="store_true",
                            help="Re-process docs that already have status=done")
        parser.add_argument("--only", type=str, default=None,
                            help=f"Comma-separated steps: {','.join(sorted(self.ALL_STEPS))}")
        parser.add_argument("--imdb-id", type=str, default=None,
                            help="Process only this imdb_id (or comma-separated list)")
        parser.add_argument("--dry-run", action="store_true",
                            help="Process but do not write to MongoDB; prints update preview")
        parser.add_argument("--shutdown-after", action="store_true")
        parser.add_argument("--notify", action="store_true")
        return parser

    def get_query(
        self,
        rerun: bool = False,
        imdb_id: str | list[str] | None = None,
        only: set[str] | None = None,
    ) -> dict:
        base = dict(self.BASE_FILTER)

        if imdb_id:
            if isinstance(imdb_id, str):
                base["imdb_id"] = imdb_id
            else:
                base["imdb_id"] = {"$in": list(imdb_id)}
            return base

        if rerun:
            if only:
                for s in only:
                    base[f"_meta.status.{self.PIPELINE_NAME}.{s}"] = "done"
            else:
                base[f"_meta.status.{self.PIPELINE_NAME}"] = {"$type": "object"}
        else:
            if only:
                for s in only:
                    base[f"_meta.status.{self.PIPELINE_NAME}.{s}"] = {"$exists": False}
            else:
                base[f"_meta.status.{self.PIPELINE_NAME}"] = {"$exists": False}

        return base

    def run(self) -> None:
        parser = self.make_parser()
        args = parser.parse_args()

        # Parse --imdb-id
        imdb_id_arg: str | list[str] | None = args.imdb_id
        if imdb_id_arg and "," in imdb_id_arg:
            imdb_id_arg = [s.strip() for s in imdb_id_arg.split(",") if s.strip()]

        # Parse --only
        only: set[str] | None = None
        if args.only:
            only = {s.strip() for s in args.only.split(",")}
            unknown = only - self.ALL_STEPS
            if unknown:
                parser.error(f"Unknown steps: {unknown}. Choose from {sorted(self.ALL_STEPS)}")

        steps_to_run = only or self.ALL_STEPS

        conn = MongoConnection()
        coll = conn.get_collection(self.COLLECTION)

        self.setup(args)

        query = self.get_query(rerun=args.rerun, imdb_id=imdb_id_arg, only=only)
        if imdb_id_arg or args.dry_run:
            print(f"Query: {query}")

        # --rerun: pre-clear status keys so docs drop out after re-processing
        if args.rerun and not args.dry_run and not imdb_id_arg:
            if only:
                # Only unset specific step sub-keys
                unset_spec = {f"_meta.status.{self.PIPELINE_NAME}.{s}": "" for s in only}
            else:
                # Unset the entire parent key (removes all sub-keys with it)
                unset_spec = {f"_meta.status.{self.PIPELINE_NAME}": ""}
            coll.update_many(query, {"$unset": unset_spec})
            query = self.get_query(rerun=False, imdb_id=imdb_id_arg, only=only)

        total = coll.count_documents(query)
        effective = min(total, args.limit) if args.limit else total
        num_chunks = math.ceil(effective / args.chunk_size) if effective else 0

        mode = "rerun" if args.rerun else "new"
        if args.dry_run:
            mode += " (dry-run)"
        if imdb_id_arg:
            mode += f" | imdb_id={imdb_id_arg}"
        step_label = ",".join(sorted(only)) if only else "all"
        print(f"Mode: {mode} | Steps: {step_label}")
        print(f"Documents: {effective}  (chunk size {args.chunk_size}, {num_chunks} chunks)\n")

        pbar = tqdm(total=effective, desc=self.PIPELINE_NAME, unit="doc")
        processed = 0

        for _ in range(num_chunks):
            fetch = min(args.chunk_size, effective - processed)
            docs = list(coll.find(query).limit(fetch))
            if not docs:
                break

            results = self.process_chunk(docs, steps_to_run)

            ops = []
            for doc in docs:
                update = self.build_update(doc, results, only)
                status_val = "done" if update else "skipped"
                for s in (only or self.ALL_STEPS):
                    update[f"_meta.status.{self.PIPELINE_NAME}.{s}"] = status_val
                update["_meta.updated_at"] = datetime.utcnow()
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update}))

                if args.dry_run:
                    print(f"\n[dry-run] {doc.get('imdb_id', doc['_id'])} would set:")
                    for k, v in update.items():
                        preview = v
                        if isinstance(v, list):
                            preview = json.dumps(v, default=str, indent=2)
                        elif isinstance(v, str) and len(v) > 120:
                            preview = v[:117] + "..."
                        print(f"  {k}: {preview}")

            if ops and not args.dry_run:
                coll.bulk_write(ops, ordered=False)

            processed += len(docs)
            pbar.update(len(docs))

        pbar.close()
        print(f"\nDone. Processed {processed} documents.")

        self.teardown()

        if args.shutdown_after or args.notify:
            cleanup(shutdown=args.shutdown_after, notify=args.notify)

        conn.close()
