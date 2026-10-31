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

One notification is accepted per manifest; pending/uncertain delivery is
reconciled against actual user-message records in this thread's own rollout.
An uncertain notification is not blindly resent. Data remains durable even
when wakeup delivery needs reconciliation; do not claim exactly-once agent
processing. Acknowledging terminal transport is not semantic acceptance.

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
