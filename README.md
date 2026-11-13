# ProComm MongoDB lease pattern

Recommended reference implementation for coordinating scheduled AKS workers.
One persistent document per task; one unique owner token per acquisition; all
actual lease deadlines are calculated with MongoDB server time. No expression
is used in an upsert filter. No `.env` configuration is needed.

## Why this approach

1. Bootstrap a missing task with an already-expired placeholder. An equality-only
   upsert with `$setOnInsert` leaves existing leases unchanged. Bootstrap can also
   run during deployment when task keys are known.
2. Acquire an expired lease atomically with `find_one_and_update`, without upsert.
   The update pipeline sets `expires_at` to `$$NOW` plus the lease duration and
   increments a generation counter. Only a returned lease grants permission.
3. Renew only when the owner token, generation and unexpired deadline all match.
   An expired lease must be reacquired with a new owner token.
4. Persist results in a short transaction that actually modifies the matching
   lease and writes the results. A read-only ownership check is insufficient.
5. Release by expiring the document. Never delete lease documents or put a TTL
   index on this collection: retaining the document preserves generations.

Use TTL separately for findings/history retention if required. The built-in
unique `_id` index is enough for this lease lookup. Do not manually rewrite the
lease schema or delete active lease documents.

## Run

Python 3.10+, a MongoDB replica set (Atlas recommended), and MongoDB 5.0+ for
`$dateAdd`. Use the team's supported MongoDB/driver versions; this dependency
range is not a managed-identity driver compatibility guarantee.

```bash
python -m pip install -r requirements.txt
python example.py
```

The example prompts for a connection string without echoing it. It writes to
`procomm_lease_example.leases` and `.findings`. Use a dedicated development
database and an identity with the required read/insert/update privileges.
The MongoClient may instead be created with their workload identity configuration.

## Integrating a scheduled agent

Choose a key that matches the exclusivity boundary. For example,
`program-A:delivery-monitor` prevents overlapping runs for that task across
schedule windows. Including a window in the key allows different windows to run
concurrently. Avoid a global key that serializes every program and agent.

Call `acquire` at the start. On `None`, skip or retry later with bounded jitter.
On any database error, do not proceed with protected work. Run external queries
and model calls outside transactions. Renew periodically, for example every two
minutes for a ten-minute lease; implement that in the worker's scheduler or
heartbeat mechanism. Stop initiating work if renewal fails or is uncertain.

Pass the completed findings to `run_guarded`. Every database write in its
callback must use the supplied session and the same MongoClient. The transaction
callback may run more than once: keep it short and deterministic, and never call
an LLM, Splunk, email, or remediation endpoint from it. The sample performs one
renewal only; it is not a production heartbeat scheduler.

Use stable finding IDs/unique business keys to make reprocessing idempotent.
Renewing a lease does not record that a schedule window has completed. Keep a
separate completion/checkpoint record if completed windows must be skipped.

## Failover and correctness boundaries

The module requests majority acknowledgement for standalone lease writes and
transaction commits. The example enables retryable writes. Driver transaction
handling retries eligible transaction errors and commit ambiguity. Errors that
escape must be handled by the worker; never turn an exception into permission.
An acquisition may have succeeded even if its response was lost. This module
fails closed: a new call uses a new token and may wait for the unseen lease to
expire. This trades a bounded delay for a simpler ownership protocol.

The transactional guard mutates the lease so takeover and protected result writes
conflict or serialize. It checks expiry at guard time, not continuously through
commit. Keep the transaction short and comfortably inside the remaining lease
duration; renew immediately beforehand. This is coordinated database write
ownership, not a guarantee that all work stops at the exact expiration instant.

Concurrent heartbeats also modify the lease and can cause transaction conflicts.
Pause/join the local heartbeat before the short guarded save, renew, then save.
Other workers must follow the same lease protocol for these guarantees to apply.

The generation is useful as a fencing token only if a downstream system actually
validates it. It does not by itself protect external actions. Future remediation
needs its own idempotency/fencing or transactional-outbox design. Do not delete
leases or reset generations while workers can still be running.

## Validation

Static compilation and packaging checked in the supplied environment. No live
Atlas execution or failover test was performed here. Before production:

- Start competing workers on one key and verify only one acquisition succeeds.
- Verify independent keys can run concurrently.
- Stop renewal and verify takeover after expiration.
- Verify an expired owner cannot renew, release a successor, or save results.
- Exercise a takeover racing with `run_guarded`; only serialized valid results
  should commit, and rejected transactions must not leave partial findings.
- Exercise primary failover, lost responses and majority-acknowledgement timeouts.
- Replay the same input and verify stable finding IDs avoid duplicate results.

## References

- https://www.mongodb.com/docs/manual/tutorial/update-documents-with-aggregation-pipeline/
- https://www.mongodb.com/docs/manual/core/transactions-production-consideration/
- https://www.mongodb.com/docs/manual/core/retryable-writes/
- https://www.mongodb.com/docs/manual/reference/write-concern/
