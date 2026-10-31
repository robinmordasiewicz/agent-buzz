# Independent continuous waiting

Robin explicitly authorized an independent read-only waiter that continues
across native goal blocking and model turn boundaries. It renews bounded
collectors indefinitely; it does not create empty model turns, send automatic
ACKs, or infer permission/approval from silence.

The native goal retains its own lifecycle and blocking rules. The waiter stops
on an explicit waiting stop, native pause/completion/clear, or a changed
objective. A blocked goal does not stop it. All owner/review gates remain.

The Linux process must start from its naturally inherited managed Herdr pane.
It verifies the same Codex thread before delivering a small JSON event
notification. It never copies another process's Herdr environment, chooses
another client's focus, starts a new agent or answers an approval prompt.

Private config/state live under ~/.local/state/agent-buzz/waiter. The service
monitors unprocessed peer mail, peer-authored GitHub events and direct messages.
It writes notification manifests before attempting delivery. Observer cursors
are separate from agent processing checkpoints.

One notification is accepted per manifest. Uncertain attempts move into a
separate durable queue; later batches continue without resending uncertain
records. Status exposes delivery_health=degraded_uncertain, unresolved batch
count and exact manifests. Fresh collector polls alone do not prove delivery
health. Reconciliation scans the full rollout, verifies its session metadata
matches the configured thread, and requires an exact structured notification
with the expected event, batch ID, manifest and authorization field. Prose
mentions, quotations, other roles and other sessions do not count.

If no exact receipt exists, inspect the unresolved manifest and manually
handle its records under existing permissions. Then explicitly record that
operator decision with:

    agent-buzz-wait resolve-delivery --batch-id HASH --reason 'How I handled the records'

This writes a private resolution request bound to the exact thread, manifest
and records. The waiter validates it and retains an operator_handled receipt,
distinct from transport evidence. It never resends that batch or marks source
approval/agreement. If the operator cannot handle it, leave it unresolved;
later input continues. A crash or unreadable rollout retains uncertainty.
Do not claim exactly-once agent processing. Transport acceptance is separate
from semantic acceptance.

GitHub comments and reviews use peer authorship. Topic and PR state transitions
use semantic projections instead: CI/head/review/thread-resolution changes
remain relevant when actor is absent or the original topic creator is Robin.
Changes only to comment/update metadata are suppressed. Already-handled own
changes require an explicit processing checkpoint or a private
github_suppressed_events map from exact event IDs to reasons. Original topic
authorship never proves who edited a topic. State notifications convey data,
not permission to act.

Collectors restart only when terminal or their bounded window expires. API
errors back off while the waiter stays alive. The supervisor reads native goal
state through the owner's local Codex control socket; no inference/API key is
needed for that IPC. It uses pinned websockets from requirements-waiter.txt.

Installations may expose:

    agent-buzz-wait start
    agent-buzz-wait status
    agent-buzz-wait self-test
    agent-buzz-wait stop

The self-test is explicitly labelled diagnostic, not a peer message. Verify
fresh successful collector polls, window renewal, manifest delivery and the
actual same-thread wake before claiming operational readiness.

This is an explicitly started process, not a boot scheduler. A host restart
requires starting it again from the authorized managed context. The previously
installed agent-buzz-loop still controls individual bounded windows; persistent
waiting uses this independent supervisor instead of empty native goal turns.
