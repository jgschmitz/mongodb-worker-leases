"""Small runnable example; prompts for the URI and does not use .env files."""
from getpass import getpass

from pymongo import MongoClient
from mongo_lease import MongoLeaseStore


def main():
    uri = getpass("Atlas connection string: ")
    with MongoClient(uri, retryWrites=True, serverSelectionTimeoutMS=30000) as client:
        db = client["procomm_lease_example"]
        leases = MongoLeaseStore(db, duration_seconds=600)
        # Use a stable key for the work being protected. This example prevents
        # overlap for one program/component, including across schedule windows.
        lease = leases.acquire("program-demo:delivery-monitor")
        if lease is None:
            print("Another execution owns this task; skipping.")
            return

        try:
            # Query Splunk/App Insights and call models OUTSIDE the transaction.
            # This example has no external calls and only prepares a sample.
            finding = {"status": "sample", "summary": "Example investigation"}
            leases.renew(lease)  # For longer work, renew well before expiry.

            def save(session):
                # Stable business identifier makes repeated scheduled/replayed
                # writes idempotent. Replace with their real finding identifier.
                db.findings.update_one(
                    {"_id": "demo-window:delivery:issue-001"},
                    {"$set": {
                        **finding,
                        "lease_generation": lease.generation,
                    }},
                    upsert=True,
                    session=session,
                )

            leases.run_guarded(lease, save)
            print("Saved sample finding under guarded ownership.")
        finally:
            # In a service, log release errors without masking the original
            # investigation/write error. Expiration permits later takeover.
            try:
                leases.release(lease)
            except Exception as exc:
                print("Release failed; lease will expire:", type(exc).__name__)


if __name__ == "__main__":
    main()
