# Communication protocol 1.2

Explicitly acknowledged by both assistants for read-only GitHub polling and
email responses. Public GitHub participation still requires each account
owner's authorization. Existing permissions always govern actions.

Email uses one topic per [agent] subject/thread and actual replies. Robin
bottom-posts plain text after one preceding quotation. A temporary exception
permits the peer connector's new text above one automatic quotation.
The message channel never carries credentials or unrelated private records.

Issues are durable project topics and acceptance records. PRs carry code review
against a specific head. Reference exact issue/PR/comment/review URLs. Keep
current agreement, open items and action owner in substantive turns.

ACK values are received, agreed or changes_requested. A substantive request
gets one explicit ACK, which may accompany the useful answer. ACKs need no ACK;
avoid duplicate replies and acknowledgment loops. Detection/posting/storage
never implies recipient processing or agreement. CI/state changes need a
response when actionable.

FIN states the precise acceptance scope, reviewed head and unresolved work.
FIN_ACK references that FIN and explicitly agrees to closure. An approving PR
review can carry FIN_ACK if it says so. Source approval/merge and live rollout
acceptance remain separate; close issues and merge only within the agreed
scope and enforced protections. A changed head requires fresh eligible review.

During active sessions, target five-second checks of issue/PR comments, reviews,
inline threads, head and check/review state, in renewable 15-minute waiting
windows. Polling pauses for message processing/tool work and backs off for
API errors or rate limits. No unattended listener is installed after a session.
Each assistant retains processed IDs/timestamps, snapshots, head and pending
actions in its own approved private records. Reconcile before acting after
reconnect. Email remains the private rapid coordination/fallback path.

## Read-only GitHub collector

    python3 tools/watch_github.py --seconds 900 --stream

Uses the caller's authenticated gh account. Events and checkpoints are private
under ~/.local/state/agent-buzz/github by default; --state-dir separates consumers.
It observes all issues/PRs with paginated inventories, uses thin GraphQL state
and complete paginated REST comment/review reconciliation after changes, and
records events before advancing its collector checkpoint. Event files and
checkpoint renames are fsynced. Event IDs include content fingerprints so edits
are new events. Restarting before a checkpoint reuses the same event file.

The collector never posts or ACKs. A model/operator reviews pending events and
records handled event IDs in processed.json (a JSON list) after acting. Stream
mode continues collecting while pending events await handling, without replying.
API cost/rate state and check times are recorded; the interval backs off when
the query cost or remaining allowance cannot sustain the target. A new active
window is explicit, and the operator stops its processes at session end.

The required CI producer is app-bound; its workflow definition remains PR code
and needs independent review. The open-issue relationship is a check-time
snapshot. Before merge, recheck current head, checks, eligible approval and
current closing-issue relation. The owner remains trusted to administer rules.
