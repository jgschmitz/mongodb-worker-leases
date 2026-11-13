"""MongoDB-backed leases for scheduled ProComm workers. Python 3.10+."""
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern


class LeaseLost(RuntimeError):
    """Ownership expired, was released, or moved to another execution."""


@dataclass(frozen=True)
class Lease:
    key: str
    owner: str
    generation: int


class MongoLeaseStore:
    def __init__(self, database, collection_name="leases", duration_seconds=600):
        if not isinstance(duration_seconds, int) or duration_seconds < 1:
            raise ValueError("duration_seconds must be a positive integer")
        self.database = database
        self.duration = duration_seconds
        self.collection = database.get_collection(
            collection_name,
            write_concern=WriteConcern(w="majority", wtimeout=10000),
            read_preference=ReadPreference.PRIMARY,
        )

    def ensure(self, key):
        """Create an expired placeholder only. Never overwrite an existing lease."""
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a nonempty string")
        try:
            self.collection.update_one(
                {"_id": key},
                {"$setOnInsert": {
                    "owner": None,
                    "generation": 0,
                    "expires_at": datetime(1970, 1, 1, tzinfo=timezone.utc),
                    "guard_seq": 0,
                }},
                upsert=True,
            )
        except DuplicateKeyError:
            # A concurrent bootstrap can create the same _id. Acquisition below
            # still has to succeed; this error NEVER grants ownership.
            pass

    def _expiry(self):
        return {"$dateAdd": {
            "startDate": "$$NOW", "unit": "second", "amount": self.duration
        }}

    @staticmethod
    def _owned_filter(lease):
        return {
            "_id": lease.key,
            "owner": lease.owner,
            "generation": lease.generation,
            "$expr": {"$gt": ["$expires_at", "$$NOW"]},
        }

    def acquire(self, key):
        """Return a Lease, or None for contention. Database errors propagate.

        A new call uses a fresh execution token. After an ambiguous error, do
        not perform protected work; retry a fresh acquisition later. An unseen
        successful attempt may hold the task until its lease expires.
        """
        self.ensure(key)
        owner = uuid4().hex
        doc = self.collection.find_one_and_update(
            {"_id": key, "$expr": {"$lte": ["$expires_at", "$$NOW"]}},
            [{"$set": {
                "owner": {"$literal": owner},
                "generation": {"$add": ["$generation", 1]},
                "expires_at": self._expiry(),
                "acquired_at": "$$NOW",
                "renewed_at": "$$NOW",
            }}],
            upsert=False,
            return_document=ReturnDocument.AFTER,
        )
        if doc is None:
            return None
        return Lease(key, owner, doc["generation"])

    def renew(self, lease):
        """An expired owner cannot renew, even if nobody has taken over yet."""
        result = self.collection.update_one(
            self._owned_filter(lease),
            [{"$set": {"expires_at": self._expiry(), "renewed_at": "$$NOW"}}],
            upsert=False,
        )
        if result.matched_count != 1:
            raise LeaseLost(lease.key)

    def release(self, lease):
        """Expire, do not delete. An old owner cannot release its successor."""
        result = self.collection.update_one(
            self._owned_filter(lease),
            [{"$set": {"owner": None, "expires_at": "$$NOW"}}],
            upsert=False,
        )
        return result.matched_count == 1

    def run_guarded(self, lease, write_callback):
        """Guard and write MongoDB results in one SHORT transaction.

        Callback signature: write_callback(session). Every protected database
        operation MUST use session=session and the same MongoClient. The driver
        may rerun the callback. No model calls, HTTP calls or other external side
        effects belong here. Complete investigation before entering this method.
        """
        def transaction(session):
            # A real mutation forces competing takeovers/renewals to serialize
            # or conflict. A plain find() or no-op update is not this guard.
            result = self.collection.update_one(
                self._owned_filter(lease),
                {"$inc": {"guard_seq": 1}},
                session=session,
            )
            if result.matched_count != 1:
                raise LeaseLost(lease.key)
            return write_callback(session)

        with self.database.client.start_session() as session:
            return session.with_transaction(
                transaction,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern(w="majority", wtimeout=10000),
                read_preference=ReadPreference.PRIMARY,
                max_commit_time_ms=10000,
            )
