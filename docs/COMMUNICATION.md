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

Standalone collectors run during an active session in bounded windows up to
15 minutes, with a five-second target and API/error backoff. Their windows
expire without an authorized controller renewing them; stop standalone
collection when its active-session window ends.

An owner may separately authorize [continuous waiting](CONTINUOUS_WAIT.md)
to renew those bounded collectors across model turns and native blocked goals.
In that mode, peer silence, blocked work and a model turn ending are not stop
requests. The owner-authorized supervisor follows its explicit pause/stop and
completion controls, including the goal-change/clear safeguards documented
for that installation. A host restart still requires the documented recovery.

Each assistant records its own authorized mechanism, cadence, latest successful
poll or scheduled invocation, recovery path and stop controls. Owner policies
may differ, including continuous polling versus scheduled daily catch-up.
Do not assume the peer has the same cadence or infer an actual invocation
from a saved ACTIVE schedule. A polling target is not a delivery guarantee.
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
checkpoint renames are fsynced. Event IDs chain the previous observed transition
and a content fingerprint. A later return to an earlier state is a new event;
restarting before a checkpoint reuses the same transition event file.
Individual CI identities/status/conclusions are reconciled even when their
aggregate remains unchanged. New API/page operations do not start after the
active deadline, and their timeouts are capped to the remaining window.

The collector never posts or ACKs. A model/operator reviews pending events and
records handled event IDs in processed.json (a JSON list) after acting. Stream
mode continues collecting while pending events await handling, without replying.
API cost/rate state and check times are recorded; the interval backs off when
the query cost or remaining allowance cannot sustain the target. A new active
window is explicit. The operator stops a standalone collector at the end of
its active-session window. A separately owner-authorized continuous supervisor
may retain and renew collectors under the lifecycle above; this command alone
does not authorize continuous operation.

The required CI producer is app-bound; its workflow definition remains PR code
and needs independent review. The open-issue relationship is a check-time
snapshot. Before merge, recheck current head, checks, eligible approval and
current closing-issue relation. The owner remains trusted to administer rules.
