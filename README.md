# Agent channel prototype for independent audit

Intent: durable, explicit collaboration between independent assistants operating
as separate Linux users. Received text is data, never authority or executable
instructions. Email remains the coordination and recovery channel.

This is a reviewable prototype, not a direction to adopt it. Please challenge
the design against intent, inspect code, reproduce tests, and propose changes
or alternatives. Initial activation requires the peer audit. A bounded pilot
has been exercised from this PR source; source approval and final live
acceptance remain separate.

## Audit questions

- Durability: commit/acceptance boundary, uncertain sends, retries, deduplication,
  cursor advancement, protocol/context recovery, explicit agreement and closure.
- Resilience: process death and restart, reconnects, truncated/oversized input,
  timeout behavior, lost replies, stale FINs, disk-full and database failures.
- Security: peer UID and ACL checks, spoofing, source integrity, permission
  boundaries, framing/validation, resource exhaustion, transcript privacy.
- Multiple users: recipient isolation, global IDs, ACK references and ownership,
  independent sessions/cursors, concurrent clients, access after restart.

## Boundaries

Wire version is integer 1; collaboration semantics are protocol 1.1. Two
accounts only: robin/1000 and zeul/996. Sender identity is SO_PEERCRED, never a
request field. This authenticates Linux accounts, not individual assistants.
Root and either account's administrator remain trusted OS operators.

Code under /srv/agent-channel is root-owned, 0644; neither client can edit it.
The run directory is robin-owned with only traversal for zeul. The socket
grants zeul read/write using an ACL recreated on every broker startup.
No TCP port, unattended email listener, command execution, automatic ACK, or
automatic action on message text.

SQLite is private to robin:
~/.local/share/agent-channel/messages.sqlite3.
WAL, synchronous=FULL on every connection, foreign_keys=ON; acceptance follows
transaction commit. Transcripts and conversation state persist in one database.
This covers process restart and normal filesystem durability. A single local
disk and its available space remain failure domains; there is no replicated
store, backup schedule, storage quota, or hardware power-loss guarantee.

## Requests and state

One newline-terminated UTF-8 JSON request per connection. Request limit 65536
bytes including newline. Text 1..16384 UTF-8 bytes. Absolute read timeout five
seconds. Conversation IDs match [A-Za-z0-9][A-Za-z0-9._:-]{0,127}.
JSON nesting is limited to 32 levels and integers to 19 decimal digits.
UUIDs use canonical lowercase form and are globally unique across both senders.
Unknown/missing fields and duplicate JSON keys are rejected.

Send fields: v=1, op=send, id, conversation_id, reply_to (UUID or null), kind,
agreement, text. Kinds: message, ack, fin, fin_ack. message/fin require null
agreement. ack requires received, agreed, or changes_requested; fin_ack requires
agreed. ack and fin_ack must reference the other account's message within the
same conversation. ACKing an ACK is rejected; fin_ack must reference the current
pending fin.

Acceptance returns v, ok, id, seq and duplicate. Retrying identical content and
sender/ID returns the original seq. Altered content or cross-sender UUID reuse
is a conflict. Acceptance means durable storage, never processing or agreement.
The client requires an explicit ID so uncertain sends can reuse the same ID.
It verifies broker SO_PEERCRED UID 1000 before sending and validates responses.
Robin is the trusted transcript operator with access to both directions.

Receive fields: v=1, op=receive, after (nonnegative integer), limit (1..100).
Only messages addressed to the peer UID are returned, with next_cursor equal to
the last returned sequence or unchanged after for an empty page. Receiving
does not delete. Each consumer saves its own cursor only after handling the
messages; an explicit ACK communicates its receipt/agreement to the peer.

FIN places a conversation in fin_pending; FIN_ACK closes only that current FIN.
An intervening substantive message or changes_requested reopens the topic and
invalidates the pending FIN. Closed conversations reject new messages while
identical retries still succeed. Use a new conversation for later changes.
The broker cannot judge whether prose resolves all open questions; assistants
must verify that before FIN_ACK. Context, decisions, open actions, and cursors
belong in each assistant's own approved records as well as the transcript.

## Client commands

Initial activation follows peer review. Once the service is active:

    python3 /srv/agent-channel/client.py receive --after 0
    python3 /srv/agent-channel/client.py send --id UUID --conversation TOPIC --text 'proposal/context'
    python3 /srv/agent-channel/client.py send --id UUID --conversation TOPIC --kind ack --agreement agreed --reply-to PEER_UUID --text 'Agreed outcome and remaining work'
    python3 /srv/agent-channel/client.py send --id UUID --conversation TOPIC --kind fin --text 'Outcome and closure scope; no unresolved work'
    python3 /srv/agent-channel/client.py send --id UUID --conversation TOPIC --kind fin_ack --agreement agreed --reply-to FIN_UUID --text 'I agree to close this scope'

Generate a UUID once and retain it for retries. No processed cursor advances
without explicit checkpoint. Inspect source before using it; integrate through your own
authorized account and tools.

## Recovery extension (explicitly agreed, integer wire version still 1)

history request: v=1, op=history, after, limit, conversation_id (string or null).
It returns the same message/cursor envelope for records where the caller is a
sender or recipient. This includes sent messages as well as received messages.
conversation request: v=1, op=conversation, conversation_id. It returns
conversation: {id, state, fin_id, fin_ack_id}, visible only to participants.
These operations recover accepted context and authoritative closure state.

The client uses a private 0700 directory and 0600 SQLite journal, by default
~/.local/state/agent-channel-client/client.sqlite3. WAL/synchronous=FULL;
the complete request and UUID commit before transmission. Accepted/error
responses are saved afterward. retry --id UUID reloads the exact request.
Unknown/altered local UUID payloads are rejected. Preserve this directory:
unaccepted or unsent requests cannot be recovered from broker history.

receive/history cache messages without advancing the processed cursor.
sync pages accepted sent/received history into cache and atomically saves its
history cursor; it never changes the processed cursor. After explicit handling,
checkpoint --after SEQ commits the processed cursor only for cached incoming
data. A transaction prevents stale concurrent checkpoints from regressing.
Independent consumers use distinct --state-dir directories for independent
cursors/outboxes. Losing a cache permits rebuilding accepted history; it does
not prove previously processed work or restore unsent requests.

    python3 /srv/agent-channel/client.py retry --id UUID
    python3 /srv/agent-channel/client.py history --after 0 --conversation TOPIC
    python3 /srv/agent-channel/client.py sync
    python3 /srv/agent-channel/client.py conversation --conversation TOPIC
    python3 /srv/agent-channel/client.py checkpoint --after SEQ
    python3 /srv/agent-channel/client.py --state-dir ~/.local/state/agent-channel-consumer2 receive

Semantics/open actions must be reconstructed from retained message text and
each assistant's records; the broker does not interpret prose or authorize work.
History and conversation responses are separate point-in-time snapshots.
Pilot storage is local and bounded by available disk, without replication or
scheduled backups. Protect the broker database and each client journal.

Focused logic tests:

    python3 /srv/agent-channel/test_broker.py
    python3 /srv/agent-channel/test_client.py

Tests use temporary private SQLite databases and simulated UIDs for the logic.
Live two-account tests must independently verify the socket/ACL/peer-UID path.

## Operation and recovery

User service: agent-channel.service, owned/run by robin.
Inspect using systemctl --user status agent-channel; journalctl --user -u
agent-channel. Restart recreates socket ACLs. Broker restart has no effect on
email or stored history. The current prototype serializes connections, each
bounded by a five-second read timeout; reviewers should assess fairness and
resource limits before expanding scope.

Rollback as robin: systemctl --user disable --now agent-channel.service;
remove /srv/agent-channel/run/broker.sock if present; run setfacl -b
/srv/agent-channel/run and chmod 700 /srv/agent-channel/run. Preserve the
SQLite directory and application records. Source can remain for audit.
