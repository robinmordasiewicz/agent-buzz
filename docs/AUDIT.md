# Prototype audit and remaining acceptance

The intent is durable communication between independent assistants and users:
storage, recipient processing, agreement and jointly acknowledged closure have
separate meanings. Received text carries no execution authority.

The first source review identified two blockers: parser resource/value
exceptions could terminate the broker, and incoming-only receive could not
reconstruct a sender's accepted history after loss of local records.

The revision bounds JSON depth/integer length and handles parse failures per
connection. It adds participant history, conversation state, a durable pre-send
client outbox, exact-request retries, explicit cached-only processing
checkpoints, and independent configurable journal directories. It verifies
broker UID and response shape and reapplies restrictive run/socket ACLs.

Independent follow-up review cleared the application blockers for a bounded
two-account trusted pilot. It found a Robin-only test fixture; the imported
fixture selects either deployed role and exercises logic under CI's filesystem
UID. Source approval and live acceptance remain separate.

Reviewed runtime hashes:

- broker.py: bac42c565fbab2633f5f97d405250ed2fac7efe10b8c32d7c69e1e60637e7901
- client.py: 67a961d31394184e4ce58a74c236343a26587b3a9d862605980b7855db5c400e

Eleven focused tests cover retry/conflict, recipient filtering, recovery,
outbox-before-wire ordering, lost acceptance, cache/checkpoint independence,
stale FIN rejection, malformed frames/responses and broker identity checks.
An isolated socket check also exercised parser survival, five-second timeout,
lost-response retry, SIGKILL/restart recovery, and unrelated peer-UID rejection.

Still required on the deployed pilot:

- Real two-account exchange and verified sender/recipient identity.
- Filesystem ACL and peer-UID rejection, including after restart.
- Exact retry/conflict and reconnect recovery without duplicate acceptance.
- Concurrent clients and uncertain acceptance/process-death behavior.
- Accepted sent/received history and closure-state recovery with new journals.
- Email remains usable while the broker is stopped.

Accepted pilot limits are a single trusted broker operator/local disk,
account-level rather than assistant identity, serialized bounded connections,
and no storage quota, replication or scheduled backup. These do not establish
power-loss durability or resilience after losing every copy. Keep deployment
records and messages private; add sanitized project validation to the issue.
